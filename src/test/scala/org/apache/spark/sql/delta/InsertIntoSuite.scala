/*
 * Copyright 2019 Databricks, Inc.
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

import io.delta.DeltaExtensions
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, SaveMode}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.internal.SQLConf.{PARTITION_OVERWRITE_MODE, PartitionOverwriteMode}
import org.apache.spark.sql.test.SharedSparkSession

class InsertIntoSQLSuite extends InsertIntoTests(false, true) {
  import testImplicits._
  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.session", classOf[DeltaCatalog].getName)
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.sql.extensions", classOf[DeltaExtensions].getName)
  }

  protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val tmpView = "tmp_view"
    withTempView(tmpView) {
      insert.createOrReplaceTempView(tmpView)
      val overwrite = if (mode == SaveMode.Overwrite) "OVERWRITE" else "INTO"
      sql(s"INSERT $overwrite TABLE $tableName SELECT * FROM $tmpView")
    }
  }

  override def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(spark.table(tableName), expected)
  }

  override protected val catalogAndNamespace: String = ""
  override protected val v2Format: String = "delta"
}

class InsertIntoSQLByPathSuite extends InsertIntoTests(false, true) {
  import testImplicits._
  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.session", classOf[DeltaCatalog].getName)
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.sql.extensions", classOf[DeltaExtensions].getName)
  }

  protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val tmpView = "tmp_view"
    withTempView(tmpView) {
      insert.createOrReplaceTempView(tmpView)
      val overwrite = if (mode == SaveMode.Overwrite) "OVERWRITE" else "INTO"
      val ident = spark.sessionState.sqlParser.parseTableIdentifier(tableName)
      val catalogTable = spark.sessionState.catalog.getTableMetadata(ident)
      sql(s"INSERT $overwrite TABLE delta.`${catalogTable.location}` SELECT * FROM $tmpView")
    }
  }

  override def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(spark.table(tableName), expected)
  }

  override protected val catalogAndNamespace: String = ""
  override protected val v2Format: String = "delta"
}

class InsertIntoDataFrameSuite extends InsertIntoTests(false, false) {

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.session", classOf[DeltaCatalog].getName)
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.sql.extensions", classOf[DeltaExtensions].getName)
  }

  override def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(spark.table(tableName), expected)
  }

  override protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val dfw = insert.write.format(v2Format)
    if (mode != null) {
      dfw.mode(mode)
    }
    dfw.insertInto(tableName)
  }

  override protected val catalogAndNamespace: String = ""
  override protected val v2Format: String = "delta"
}

class InsertIntoDataFrameByPathSuite extends InsertIntoTests(false, false) {

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.session", classOf[DeltaCatalog].getName)
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.sql.extensions", classOf[DeltaExtensions].getName)
  }

  override def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(spark.table(tableName), expected)
  }

  override protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val dfw = insert.write.format(v2Format)
    if (mode != null) {
      dfw.mode(mode)
    }
    val ident = spark.sessionState.sqlParser.parseTableIdentifier(tableName)
    val catalogTable = spark.sessionState.catalog.getTableMetadata(ident)
    dfw.insertInto(s"delta.`${catalogTable.location}`")
  }

  override protected val catalogAndNamespace: String = ""
  override protected val v2Format: String = "delta"
}

abstract class InsertIntoTests(
    override protected val supportsDynamicOverwrite: Boolean,
    override protected val includeSQLOnlyTests: Boolean) extends InsertIntoSQLOnlyTests {

  import testImplicits._

  override def afterEach(): Unit = {
    spark.catalog.listTables().collect().foreach(t =>
      sql(s"drop table ${t.name}"))
  }

  /**
   * Insert data into a table using the insertInto statement. Implementations can be in SQL
   * ("INSERT") or using the DataFrameWriter (`df.write.insertInto`).
   */
  protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode = null): Unit

  test("insertInto: append") {
    val t1 = s"${catalogAndNamespace}tbl"
    sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    doInsert(t1, df)
    verifyTable(t1, df)
  }

  test("insertInto: append by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    val dfr = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("data", "id")

    doInsert(t1, dfr)
    verifyTable(t1, df)
  }

  test("insertInto: append partitioned table") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      doInsert(t1, df)
      verifyTable(t1, df)
    }
  }

  test("insertInto: overwrite non-partitioned table") {
    val t1 = s"${catalogAndNamespace}tbl"
    sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    val df2 = Seq((4L, "d"), (5L, "e"), (6L, "f")).toDF("id", "data")
    doInsert(t1, df)
    doInsert(t1, df2, SaveMode.Overwrite)
    verifyTable(t1, df2)
  }

  test("insertInto: overwrite partitioned table in static mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
      val t1 = s"${catalogAndNamespace}tbl"
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
      val init = Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data")
      doInsert(t1, init)

      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      doInsert(t1, df, SaveMode.Overwrite)
      verifyTable(t1, df)
    }
  }


  test("insertInto: overwrite by position") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
      val t1 = s"${catalogAndNamespace}tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
        val init = Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data")
        doInsert(t1, init)

        val dfr = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("data", "id")
        doInsert(t1, dfr, SaveMode.Overwrite)

        val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
        verifyTable(t1, df)
      }
    }
  }

  test("insertInto: fails when missing a column") {
    val t1 = s"${catalogAndNamespace}tbl"
    sql(s"CREATE TABLE $t1 (id bigint, data string, missing string) USING $v2Format")
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    val exc = intercept[AnalysisException] {
      doInsert(t1, df)
    }

    verifyTable(t1, Seq.empty[(Long, String, String)].toDF("id", "data", "missing"))
    assert(exc.getMessage.contains("not enough data columns"))
  }

  test("insertInto: fails when an extra column is present but can evolve schema") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      val df = Seq((1L, "a", "mango")).toDF("id", "data", "fruit")
      val exc = intercept[AnalysisException] {
        doInsert(t1, df)
      }

      verifyTable(t1, Seq.empty[(Long, String)].toDF("id", "data"))
      assert(exc.getMessage.contains(s"mergeSchema"))

      withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
        doInsert(t1, df)
      }
      verifyTable(t1, Seq((1L, "a", "mango")).toDF("id", "data", "fruit"))
    }
  }

  test("insertInto: schema enforcement") {
    val t1 = s"${catalogAndNamespace}tbl"
    sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
    val df = Seq(("a", 1L), ("b", 2L), ("c", 3L)).toDF("id", "data") // reverse order

    intercept[AnalysisException] {
      doInsert(t1, df)
    }

    verifyTable(t1, Seq.empty[(Long, String)].toDF("id", "data"))

    intercept[AnalysisException] {
      doInsert(t1, df, SaveMode.Overwrite)
    }

    verifyTable(t1, Seq.empty[(Long, String)].toDF("id", "data"))
  }

  dynamicOverwriteTest("insertInto: overwrite partitioned table in dynamic mode") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
      val init = Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data")
      doInsert(t1, init)

      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      doInsert(t1, df, SaveMode.Overwrite)

      verifyTable(t1, df.union(sql("SELECT 4L, 'keep'")))
    }
  }

  dynamicOverwriteTest("insertInto: overwrite partitioned table in dynamic mode by position") {
    val t1 = s"${catalogAndNamespace}tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
      val init = Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data")
      doInsert(t1, init)

      val dfr = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("data", "id")
      doInsert(t1, dfr, SaveMode.Overwrite)

      val df = Seq((1L, "a"), (2L, "b"), (3L, "c"), (4L, "keep")).toDF("id", "data")
      verifyTable(t1, df)
    }
  }
}

trait InsertIntoSQLOnlyTests
  extends QueryTest
    with SharedSparkSession
    with BeforeAndAfter {

  import testImplicits._

  /** Check that the results in `tableName` match the `expected` DataFrame. */
  protected def verifyTable(tableName: String, expected: DataFrame): Unit

  protected val v2Format: String
  protected val catalogAndNamespace: String

  /**
   * Whether dynamic partition overwrites are supported by the `Table` definitions used in the
   * test suites. Tables that leverage the V1 Write interface do not support dynamic partition
   * overwrites.
   */
  protected val supportsDynamicOverwrite: Boolean

  /** Whether to include the SQL specific tests in this trait within the extending test suite. */
  protected val includeSQLOnlyTests: Boolean

  private def withTableAndData(tableName: String)(testFn: String => Unit): Unit = {
    withTable(tableName) {
      val viewName = "tmp_view"
      val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
      df.createOrReplaceTempView(viewName)
      withTempView(viewName) {
        testFn(viewName)
      }
    }
  }

  protected def dynamicOverwriteTest(testName: String)(f: => Unit): Unit = {
    test(testName) {
      try {
        withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
          f
        }
        if (!supportsDynamicOverwrite) {
          fail("Expected failure from test, because the table doesn't support dynamic overwrites")
        }
      } catch {
        case a: AnalysisException if !supportsDynamicOverwrite =>
          assert(a.getMessage.contains("does not support dynamic overwrite"))
      }
    }
  }

  if (includeSQLOnlyTests) {
    test("InsertInto: when the table doesn't exist") {
      val t1 = s"${catalogAndNamespace}tbl"
      val t2 = s"${catalogAndNamespace}tbl2"
      withTableAndData(t1) { _ =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
        val e = intercept[AnalysisException] {
          sql(s"INSERT INTO $t2 VALUES (2L, 'dummy')")
        }
        assert(e.getMessage.contains(t2))
        assert(e.getMessage.contains("Table not found"))
      }
    }

    test("InsertInto: append to partitioned table - static clause") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
        sql(s"INSERT INTO $t1 PARTITION (id = 23) SELECT data FROM $view")
        verifyTable(t1, sql(s"SELECT 23, data FROM $view"))
      }
    }

    test("InsertInto: static PARTITION clause fails with non-partition column") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (data)")

        val exc = intercept[AnalysisException] {
          sql(s"INSERT INTO TABLE $t1 PARTITION (id=1) SELECT data FROM $view")
        }

        verifyTable(t1, spark.emptyDataFrame)
        assert(exc.getMessage.contains(
          "PARTITION clause cannot contain a non-partition column name"))
        assert(exc.getMessage.contains("id"))
      }
    }

    test("InsertInto: dynamic PARTITION clause fails with non-partition column") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")

        val exc = intercept[AnalysisException] {
          sql(s"INSERT INTO TABLE $t1 PARTITION (data) SELECT * FROM $view")
        }

        verifyTable(t1, spark.emptyDataFrame)
        assert(exc.getMessage.contains(
          "PARTITION clause cannot contain a non-partition column name"))
        assert(exc.getMessage.contains("data"))
      }
    }

    test("InsertInto: overwrite - dynamic clause - static mode") {
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
        val t1 = s"${catalogAndNamespace}tbl"
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
          sql(s"INSERT INTO $t1 VALUES (2L, 'dummy'), (4L, 'also-deleted')")
          sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id) SELECT * FROM $view")
          verifyTable(t1, Seq(
            (1, "a"),
            (2, "b"),
            (3, "c")).toDF())
        }
      }
    }

    dynamicOverwriteTest("InsertInto: overwrite - dynamic clause - dynamic mode") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy'), (4L, 'keep')")
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id) SELECT * FROM $view")
        verifyTable(t1, Seq(
          (1, "a"),
          (2, "b"),
          (3, "c"),
          (4, "keep")).toDF("id", "data"))
      }
    }

    test("InsertInto: overwrite - missing clause - static mode") {
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
        val t1 = s"${catalogAndNamespace}tbl"
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
          sql(s"INSERT INTO $t1 VALUES (2L, 'dummy'), (4L, 'also-deleted')")
          sql(s"INSERT OVERWRITE TABLE $t1 SELECT * FROM $view")
          verifyTable(t1, Seq(
            (1, "a"),
            (2, "b"),
            (3, "c")).toDF("id", "data"))
        }
      }
    }

    dynamicOverwriteTest("InsertInto: overwrite - missing clause - dynamic mode") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy'), (4L, 'keep')")
        sql(s"INSERT OVERWRITE TABLE $t1 SELECT * FROM $view")
        verifyTable(t1, Seq(
          (1, "a"),
          (2, "b"),
          (3, "c"),
          (4, "keep")).toDF("id", "data"))
      }
    }

    test("InsertInto: overwrite - static clause") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string, p1 int) " +
          s"USING $v2Format PARTITIONED BY (p1)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 23), (4L, 'keep', 2)")
        verifyTable(t1, Seq(
          (2L, "dummy", 23),
          (4L, "keep", 2)).toDF("id", "data", "p1"))
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p1 = 23) SELECT * FROM $view")
        verifyTable(t1, Seq(
          (1, "a", 23),
          (2, "b", 23),
          (3, "c", 23),
          (4, "keep", 2)).toDF("id", "data", "p1"))
      }
    }

    test("InsertInto: overwrite - mixed clause - static mode") {
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
        val t1 = s"${catalogAndNamespace}tbl"
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
            s"USING $v2Format PARTITIONED BY (id, p)")
          sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'also-deleted', 2)")
          sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id, p = 2) SELECT * FROM $view")
          verifyTable(t1, Seq(
            (1, "a", 2),
            (2, "b", 2),
            (3, "c", 2)).toDF("id", "data", "p"))
        }
      }
    }

    test("InsertInto: overwrite - mixed clause reordered - static mode") {
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
        val t1 = s"${catalogAndNamespace}tbl"
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
            s"USING $v2Format PARTITIONED BY (id, p)")
          sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'also-deleted', 2)")
          sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p = 2, id) SELECT * FROM $view")
          verifyTable(t1, Seq(
            (1, "a", 2),
            (2, "b", 2),
            (3, "c", 2)).toDF("id", "data", "p"))
        }
      }
    }

    test("InsertInto: overwrite - implicit dynamic partition - static mode") {
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
        val t1 = s"${catalogAndNamespace}tbl"
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
            s"USING $v2Format PARTITIONED BY (id, p)")
          sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'also-deleted', 2)")
          sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p = 2) SELECT * FROM $view")
          verifyTable(t1, Seq(
            (1, "a", 2),
            (2, "b", 2),
            (3, "c", 2)).toDF("id", "data", "p"))
        }
      }
    }

    dynamicOverwriteTest("InsertInto: overwrite - mixed clause - dynamic mode") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
          s"USING $v2Format PARTITIONED BY (id, p)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'keep', 2)")
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p = 2, id) SELECT * FROM $view")
        verifyTable(t1, Seq(
          (1, "a", 2),
          (2, "b", 2),
          (3, "c", 2),
          (4, "keep", 2)).toDF("id", "data", "p"))
      }
    }

    dynamicOverwriteTest("InsertInto: overwrite - mixed clause reordered - dynamic mode") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
          s"USING $v2Format PARTITIONED BY (id, p)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'keep', 2)")
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id, p = 2) SELECT * FROM $view")
        verifyTable(t1, Seq(
          (1, "a", 2),
          (2, "b", 2),
          (3, "c", 2),
          (4, "keep", 2)).toDF("id", "data", "p"))
      }
    }

    dynamicOverwriteTest("InsertInto: overwrite - implicit dynamic partition - dynamic mode") {
      val t1 = s"${catalogAndNamespace}tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
          s"USING $v2Format PARTITIONED BY (id, p)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'keep', 2)")
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p = 2) SELECT * FROM $view")
        verifyTable(t1, Seq(
          (1, "a", 2),
          (2, "b", 2),
          (3, "c", 2),
          (4, "keep", 2)).toDF("id", "data", "p"))
      }
    }

    test("InsertInto: overwrite - multiple static partitions - dynamic mode") {
      // Since all partitions are provided statically, this should be supported by everyone
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
        val t1 = s"${catalogAndNamespace}tbl"
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
            s"USING $v2Format PARTITIONED BY (id, p)")
          sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'keep', 2)")
          sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id = 2, p = 2) SELECT data FROM $view")
          verifyTable(t1, Seq(
            (2, "a", 2),
            (2, "b", 2),
            (2, "c", 2),
            (4, "keep", 2)).toDF("id", "data", "p"))
        }
      }
    }
  }
}
