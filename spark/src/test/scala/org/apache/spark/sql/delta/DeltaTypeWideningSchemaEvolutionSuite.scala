/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest, SaveMode}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.types._


/**
 * Suite covering widening columns and fields type as part of automatic schema evolution when the
 * type widening table feature is supported.
 */
class DeltaTypeWideningSchemaEvolutionSuite
    extends QueryTest
    with DeltaDMLTestUtils
    with DeltaSQLCommandTest
    with DeltaTypeWideningTestMixin
    with DeltaMergeIntoTypeWideningSchemaEvolutionTests {

  protected override def sparkConf: SparkConf = {
    super.sparkConf
      .set(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key, "true")
  }
}

/**
 * Tests covering type widening during schema evolution in MERGE INTO.
 */
trait DeltaMergeIntoTypeWideningSchemaEvolutionTests
    extends MergeIntoSQLTestUtils
    with MergeIntoSchemaEvolutionMixin
    with DeltaTypeWideningTestCases {
  self: QueryTest with DeltaTypeWideningTestMixin with DeltaDMLTestUtils =>

  import testImplicits._

  for {
    testCase <- supportedTestCases
  } {
    test(s"automatic type widening in merge ${testCase.fromType.sql} -> ${testCase.toType.sql}") {
      withTable("source") {
        testCase.additionalValuesDF.write.format("delta").saveAsTable("source")
        append(testCase.initialValuesDF)

        // We mainly want to ensure type widening is correctly applied to the schema. We use a
        // trivial insert only merge to make it easier to validate results.
        executeMerge(
          tgt = s"delta.`$tempPath` t",
          src = "source",
          cond = "0 = 1",
          clauses = insert("*"))

        assert(readDeltaTable(tempPath).schema("value").dataType === testCase.toType)
        checkAnswer(
          readDeltaTable(tempPath).select("value").sort("value"),
          testCase.expectedResult.select($"value".cast(testCase.toType)).sort("value"))
      }
    }
  }

  for {
    testCase <- unsupportedTestCases
  } {
    test(s"unsupported automatic type widening in merge " +
      s"${testCase.fromType.sql} -> ${testCase.toType.sql}") {
      withTable("source") {
        testCase.additionalValuesDF.write.format("delta").saveAsTable("source")
        append(testCase.initialValuesDF)

        // Test cases for some of the unsupported type changes may overflow while others only have
        // values that can be implicitly cast to the narrower type - e.g. double ->float.
        // We set storeAssignmentPolicy to LEGACY to ignore overflows, this test only ensures
        // that the table schema didn't evolve.
        withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.LEGACY.toString) {
          executeMerge(
            tgt = s"delta.`$tempPath` t",
            src = "source",
            cond = "0 = 1",
            clauses = insert("*"))
          assert(readDeltaTable(tempPath).schema("value").dataType === testCase.fromType)
        }
      }
    }
  }

  test("type widening isn't applied in merge when schema evolution is disabled") {
    withTable("source") {
      sql(s"CREATE TABLE delta.`$tempPath` (a short) USING DELTA")
      sql("CREATE TABLE source (a int) USING DELTA")
      sql("INSERT INTO source VALUES (1), (2)")

      withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "false") {
        // Merge integer values. This should succeed and downcast the values to short.
        executeMerge(
          tgt = s"delta.`$tempPath` t",
          src = "source",
          cond = "0 = 1",
          clauses = insert("*")
        )
        assert(readDeltaTable(tempPath).schema("a").dataType === ShortType)
        checkAnswer(readDeltaTable(tempPath),
          Seq(1, 2).toDF("a").select($"a".cast(ShortType)))
      }

      // Check that we would actually widen if schema evolution was enabled.
      withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
        executeMerge(
          tgt = s"delta.`$tempPath` t",
          src = "source",
          cond = "0 = 1",
          clauses = insert("*")
        )
        assert(readDeltaTable(tempPath).schema("a").dataType === IntegerType)
        checkAnswer(readDeltaTable(tempPath), Seq(1, 2, 1, 2).toDF("a"))
      }
    }
  }

  /**
   * Wrapper around testNestedStructsEvolution that constrains the result with and without schema
   * evolution to be the same: the schema is different but the values should be the same.
   */
  protected def testTypeEvolution(name: String)(
      target: Seq[String],
      source: Seq[String],
      targetSchema: StructType,
      sourceSchema: StructType,
      cond: String = "t.key = s.key",
      clauses: Seq[MergeClause] = Seq.empty,
      result: Seq[String],
      resultSchema: StructType): Unit =
    testNestedStructsEvolution(name)(
      target,
      source,
      targetSchema,
      sourceSchema,
      cond,
      clauses,
      result,
      resultWithoutEvolution = result,
      resultSchema = resultSchema)


  testTypeEvolution("change top-level column short -> int with update")(
    target = Seq("""{ "a": 0 }""", """{ "a": 10 }"""),
    source = Seq("""{ "a": 0 }""", """{ "a": 20 }"""),
    targetSchema = new StructType().add("a", ShortType),
    sourceSchema = new StructType().add("a", IntegerType),
    cond = "t.a = s.a",
    clauses = update("a = s.a + 1") :: Nil,
    result = Seq("""{ "a": 1 }""", """{ "a": 10 }"""),
    resultSchema = new StructType()
      .add("a", IntegerType, nullable = true,
        metadata = typeWideningMetadata(version = 1, from = ShortType, to = IntegerType))
  )

  testTypeEvolution("change top-level column short -> int with insert")(
    target = Seq("""{ "a": 0 }""", """{ "a": 10 }"""),
    source = Seq("""{ "a": 0 }""", """{ "a": 20 }"""),
    targetSchema = new StructType().add("a", ShortType),
    sourceSchema = new StructType().add("a", IntegerType),
    cond = "t.a = s.a",
    clauses = insert("(a) VALUES (s.a)") :: Nil,
    result = Seq("""{ "a": 0 }""", """{ "a": 10 }""", """{ "a": 20 }"""),
    resultSchema = new StructType()
      .add("a", IntegerType, nullable = true,
        metadata = typeWideningMetadata(version = 1, from = ShortType, to = IntegerType))
  )

  testTypeEvolution("updating using narrower value doesn't evolve schema")(
    target = Seq("""{ "a": 0 }""", """{ "a": 10 }"""),
    source = Seq("""{ "a": 0 }""", """{ "a": 20 }"""),
    targetSchema = new StructType().add("a", IntegerType),
    sourceSchema = new StructType().add("a", ShortType),
    cond = "t.a = s.a",
    clauses = update("a = s.a + 1") :: Nil,
    result = Seq("""{ "a": 1 }""", """{ "a": 10 }"""),
    resultSchema = new StructType().add("a", IntegerType)
  )

  testTypeEvolution("only columns in assignments are widened")(
    target = Seq("""{ "a": 0, "b": 5 }""", """{ "a": 10, "b": 15 }"""),
    source = Seq("""{ "a": 0, "b": 6 }""", """{ "a": 20, "b": 16 }"""),
    targetSchema = new StructType()
      .add("a", ShortType)
      .add("b", ShortType),
    sourceSchema = new StructType()
      .add("a", IntegerType)
      .add("b", IntegerType),
    cond = "t.a = s.a",
    clauses = update("a = s.a + 1") :: Nil,
    result = Seq(
      """{ "a": 1, "b": 5 }""", """{ "a": 10, "b": 15 }"""),
    resultSchema = new StructType()
      .add("a", IntegerType, nullable = true,
        metadata = typeWideningMetadata(version = 1, from = ShortType, to = IntegerType))
      .add("b", ShortType)
  )

  testTypeEvolution("automatic widening of struct field with struct assignment")(
    target = Seq("""{ "s": { "a": 1 } }""", """{ "s": { "a": 10 } }"""),
    source = Seq("""{ "s": { "a": 1 } }""", """{ "s": { "a": 20 } }"""),
    targetSchema = new StructType()
      .add("s", new StructType()
        .add("a", ShortType)),
    sourceSchema = new StructType()
      .add("s", new StructType()
        .add("a", IntegerType)),
    cond = "t.s.a = s.s.a",
    clauses = update("t.s.a = s.s.a + 1") :: Nil,
    result = Seq("""{ "s": { "a": 2 } }""", """{ "s": { "a": 10 } }"""),
    resultSchema = new StructType()
      .add("s", new StructType()
        .add("a", IntegerType, nullable = true,
        metadata = typeWideningMetadata(version = 1, from = ShortType, to = IntegerType)))
  )

  testTypeEvolution("automatic widening of struct field with field assignment")(
    target = Seq("""{ "s": { "a": 1 } }""", """{ "s": { "a": 10 } }"""),
    source = Seq("""{ "s": { "a": 1 } }""", """{ "s": { "a": 20 } }"""),
    targetSchema = new StructType()
      .add("s", new StructType()
        .add("a", ShortType)),
    sourceSchema = new StructType()
      .add("s", new StructType()
        .add("a", IntegerType)),
    cond = "t.s.a = s.s.a",
    clauses = update("t.s.a = s.s.a + 1") :: Nil,
    result = Seq("""{ "s": { "a": 2 } }""", """{ "s": { "a": 10 } }"""),
    resultSchema = new StructType()
      .add("s", new StructType()
        .add("a", IntegerType, nullable = true,
        metadata = typeWideningMetadata(version = 1, from = ShortType, to = IntegerType)))
  )

  testTypeEvolution("automatic widening of map value")(
    target = Seq("""{ "m": { "a": 1 } }"""),
    source = Seq("""{ "m": { "a": 2 } }"""),
    targetSchema = new StructType()
      .add("m", MapType(StringType, ShortType)),
    sourceSchema = new StructType()
      .add("m", MapType(StringType, IntegerType)),
    // Can't compare maps
    cond = "1 = 1",
    clauses = update("t.m = s.m") :: Nil,
    result = Seq("""{ "m": { "a": 2 } }"""),
    resultSchema = new StructType()
      .add("m",
        MapType(StringType, IntegerType),
        nullable = true,
        metadata = typeWideningMetadata(
          version = 1,
          from = ShortType,
          to = IntegerType,
          path = Seq("value")))
  )

  testTypeEvolution("automatic widening of array element")(
    target = Seq("""{ "a": [1, 2] }"""),
    source = Seq("""{ "a": [3, 4] }"""),
    targetSchema = new StructType()
      .add("a", ArrayType(ShortType)),
    sourceSchema = new StructType()
      .add("a", ArrayType(IntegerType)),
    cond = "t.a != s.a",
    clauses = update("t.a = s.a") :: Nil,
    result = Seq("""{ "a": [3, 4] }"""),
    resultSchema = new StructType()
      .add("a",
        ArrayType(IntegerType),
        nullable = true,
        metadata = typeWideningMetadata(
          version = 1,
          from = ShortType,
          to = IntegerType,
          path = Seq("element")))
  )

  testTypeEvolution("multiple automatic widening")(
    target = Seq("""{ "a": 1, "b": 2  }"""),
    source = Seq("""{ "a": 1, "b": 4  }""", """{ "a": 5, "b": 6  }"""),
    targetSchema = new StructType()
      .add("a", ByteType)
      .add("b", ShortType),
    sourceSchema = new StructType()
      .add("a", ShortType)
      .add("b", IntegerType),
    cond = "t.a = s.a",
    clauses = update("*") :: insert("*")  :: Nil,
    result = Seq("""{ "a": 1, "b": 4  }""", """{ "a": 5, "b": 6  }"""),
    resultSchema = new StructType()
      .add("a", ShortType, nullable = true,
        metadata = typeWideningMetadata(version = 1, from = ByteType, to = ShortType))
      .add("b", IntegerType, nullable = true,
        metadata = typeWideningMetadata(version = 1, from = ShortType, to = IntegerType))
  )
}
