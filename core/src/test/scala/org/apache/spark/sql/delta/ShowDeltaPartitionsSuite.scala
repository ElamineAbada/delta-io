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

import java.io.File
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row, SparkSession}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

trait ShowDeltaPartitionsSuiteBase extends QueryTest
  with SharedSparkSession with DeltaTestUtilsForTempViews {

  import testImplicits._

  protected def checkResult(
    result: DataFrame,
    expected: Seq[Row],
    columns: Seq[String]): Unit = {
    checkAnswer(
      result.select(columns.head, columns.tail: _*),
      expected
    )
  }

  def showDeltaPartitionsTest(f: File => String): Unit = {
    val tempDir = Utils.createTempDir()
    (1 to 10).toDF("column1")
      .withColumn("column2", col("column1") % 2)
      .write
      .format("delta")
      .partitionBy("column2")
      .save(tempDir.toString())

    checkResult(
      sql(s"SHOW PARTITIONS ${f(tempDir)}"),
      Seq(Row(0), Row(1)),
      Seq("column2"))
  }

  test("delta table: path") {
    showDeltaPartitionsTest(f => s"'${f.toString()}'")
  }

  test("delta table: delta table identifier") {
    showDeltaPartitionsTest(f => s"delta.`${f.toString()}`")
  }

  ignore("non-delta table: table name") {
    withTable("show_partitions") {
      sql(
        """
          |CREATE TABLE show_partitions(column1 INT, column2 INT)
          |USING parquet
          |PARTITIONED BY (column1)
          |COMMENT "this is a table comment"
        """.stripMargin)
      sql(
        """
          |INSERT INTO show_partitions PARTITION (column1 = 1) VALUES (1)
        """.stripMargin
      )

      sql("SHOW PARTITIONS show_partitions").show()
      /* checkResult(
        sql("SHOW PARTITIONS show_partitions"),
        Seq(Row("column1")),
        Seq("column1")) */
    }
  }

  ignore("parquet table partitions") {
    val tempDir = Utils.createTempDir()

    val spark = SparkSession
      .builder()
      .appName("test_app")
      .master("local[*]")
      .getOrCreate()

    (1 to 10).toDF("column1")
      .withColumn("column2", col("column1") % 2)
      .write
      .format("parquet")
      .partitionBy("column2")
      .saveAsTable("my_parquet_tab")

    spark.sql("show partitions my_parquet_tab").show()
  }

  test("delta table: table name") {
    withTable("show_partitions") {
      sql(
        """
          |CREATE TABLE show_partitions(column1 INT, column2 INT)
          |USING delta
          |PARTITIONED BY (column1)
          |COMMENT "describe a non delta table"
        """.stripMargin)
      sql(
        """
          |INSERT INTO show_partitions PARTITION (column1 = 1) VALUES(1)
        """.stripMargin
      )
      checkResult(
        sql("SHOW PARTITIONS show_partitions"),
        Seq(Row(1)),
        Seq("column1"))
    }
  }

  testWithTempView("show partitions on temp view") { isSQLTempView =>
    withTable("t1") {
      Seq(1, 2, 3).toDF()
        .withColumn("part", lit(1))
        .write.format("delta").saveAsTable("t1")
      val viewName = "v"
      createTempViewFromTable("t1", isSQLTempView)
      val e = intercept[AnalysisException] {
        sql(s"SHOW PARTITIONS $viewName")
      }
      assert(e.getMessage.contains(
        s"`$viewName` is a view. SHOW PARTITIONS is only supported for tables."))
    }
  }

  test("show partitions on permanent view") {
    val view = "detailTestView"
    withView(view) {
      sql(s"CREATE VIEW $view AS SELECT 1")
      val e = intercept[AnalysisException] { sql(s"SHOW PARTITIONS $view") }
      assert(e.getMessage.contains(
        "`detailTestView` is a view. SHOW PARTITIONS is only supported for tables."))
    }
  }
}

class ShowDeltaPartitionsSuite
  extends ShowDeltaPartitionsSuiteBase with DeltaSQLCommandTest
