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

import java.util.concurrent.ExecutionException

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.delta.DeltaOperations.{ManualUpdate, Truncate}
import org.apache.spark.sql.delta.actions.{DomainMetadata, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.junit.Assert._

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession

class DomainMetadataSuite
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {
  import testImplicits._

  private def sortByDomain(domainMetadatas: Seq[DomainMetadata]): Seq[DomainMetadata] =
    domainMetadatas.sortBy(_.domain)

  /**
   * A helper to validate the [[DomainMetadata]] actions can be retained during the delta state
   * reconstruction.
   *
   * @param doCheckpoint: Explicitly create a delta log checkpoint if marked as true.
   * @param doChecksum: Disable writting checksum file if marked as false.
  */
  private def validateStateReconstructionHelper(
      doCheckpoint: Boolean,
      doChecksum: Boolean): Unit = {
    val table = "testTable"
    withTable(table) {
      withSQLConf(
        DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> doChecksum.toString) {
        sql(
          s"""
             | CREATE TABLE $table(id int) USING delta
             | tblproperties
             | ('${TableFeatureProtocolUtils.propertyKey(DomainMetadataTableFeature)}' = 'enabled')
             |""".stripMargin)
        (1 to 100).toDF("id").write.format("delta").mode("append").saveAsTable(table)

        var deltaLog = DeltaLog.forTable(spark, TableIdentifier(table))
        assert(deltaLog.unsafeVolatileSnapshot.domainMetadatas.isEmpty)

        val domainMetadatas = DomainMetadata("testDomain1", Map.empty, false) ::
          DomainMetadata("testDomain2", Map("key1" -> "value1"), false) :: Nil
        deltaLog.startTransaction().commit(domainMetadatas, Truncate())
        assertEquals(sortByDomain(domainMetadatas), sortByDomain(deltaLog.update().domainMetadatas))
        assert(deltaLog.update().logSegment.checkpoint.isEmpty)

        if (doCheckpoint) {
          deltaLog.checkpoint(deltaLog.unsafeVolatileSnapshot)
          // Clear the DeltaLog cache to force creating a new DeltaLog instance which will build
          // the Snapshot from the checkpoint file.
          DeltaLog.clearCache()
          deltaLog = DeltaLog.forTable(spark, TableIdentifier(table))
          assert(deltaLog.unsafeVolatileSnapshot.logSegment.checkpoint.nonEmpty)

          assertEquals(
            sortByDomain(domainMetadatas),
            sortByDomain(deltaLog.unsafeVolatileSnapshot.domainMetadatas))
        }

        DeltaLog.clearCache()
        deltaLog = DeltaLog.forTable(spark, TableIdentifier(table))
      }
    }
  }

  // A helper to validate [[DomainMetadata]] actions can be deleted.
  private def validateDeletionHelper(doCheckpoint: Boolean, doChecksum: Boolean): Unit = {
    val table = "testTable"
    withTable(table) {
      withSQLConf(
        DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> doChecksum.toString
      ) {
        sql(
          s"""
             | CREATE TABLE $table(id int) USING delta
             | tblproperties
             | ('${TableFeatureProtocolUtils.propertyKey(DomainMetadataTableFeature)}' = 'enabled')
             |""".stripMargin)
        (1 to 100).toDF("id").write.format("delta").mode("append").saveAsTable(table)

        DeltaLog.clearCache()
        var deltaLog = DeltaLog.forTable(spark, TableIdentifier(table))
        assert(deltaLog.unsafeVolatileSnapshot.domainMetadatas.isEmpty)

        val domainMetadatas = DomainMetadata("testDomain1", Map.empty, false) ::
          DomainMetadata("testDomain2", Map("key1" -> "value1"), false) :: Nil

        deltaLog.startTransaction().commit(domainMetadatas, Truncate())
        assertEquals(sortByDomain(domainMetadatas), sortByDomain(deltaLog.update().domainMetadatas))
        assert(deltaLog.unsafeVolatileSnapshot.logSegment.checkpoint.isEmpty)

        // Delete testDomain1.
        deltaLog.startTransaction().commit(
          DomainMetadata("testDomain1", Map.empty, true) :: Nil, Truncate())
        val domainMetadatasAfterDeletion = DomainMetadata(
          "testDomain2",
          Map("key1" -> "value1"), false) :: Nil
        assertEquals(
          sortByDomain(domainMetadatasAfterDeletion),
          sortByDomain(deltaLog.update().domainMetadatas))

        // Create a new commit and validate the incrementally built snapshot state respects the
        // DomainMetadata deletion.
        deltaLog.startTransaction().commit(Nil, ManualUpdate)
        deltaLog.update()
        assertEquals(
          sortByDomain(domainMetadatasAfterDeletion),
          deltaLog.unsafeVolatileSnapshot.domainMetadatas)
        if (doCheckpoint) {
          deltaLog.checkpoint(deltaLog.unsafeVolatileSnapshot)
          assertEquals(
            sortByDomain(domainMetadatasAfterDeletion),
            deltaLog.update().domainMetadatas)
        }

        // force state reconstruction and validate it respects the DomainMetadata retention.
        DeltaLog.clearCache()
        deltaLog = DeltaLog.forTable(spark, TableIdentifier(table))
        assertEquals(
          sortByDomain(domainMetadatasAfterDeletion),
          deltaLog.update().domainMetadatas)
      }
    }
  }

  test("DomainMetadata action survives state reconstruction [w/o checkpoint, w/o checksum]") {
    validateStateReconstructionHelper(doCheckpoint = false, doChecksum = false)
  }

  test("DomainMetadata action survives state reconstruction [w/ checkpoint, w/ checksum]") {
    validateStateReconstructionHelper(doCheckpoint = true, doChecksum = true)
  }

  test("DomainMetadata action survives state reconstruction [w/ checkpoint, w/o checksum]") {
    validateStateReconstructionHelper(doCheckpoint = true, doChecksum = false)
  }

  test("DomainMetadata action survives state reconstruction [w/o checkpoint, w/ checksum]") {
    validateStateReconstructionHelper(doCheckpoint = false, doChecksum = true)
  }

  test("DomainMetadata deletion [w/o checkpoint, w/o checksum]") {
    validateDeletionHelper(doCheckpoint = false, doChecksum = false)
  }

  test("DomainMetadata deletion [w/ checkpoint, w/o checksum]") {
    validateDeletionHelper(doCheckpoint = true, doChecksum = false)
  }

  test("DomainMetadata deletion [w/o checkpoint, w/ checksum]") {
    validateDeletionHelper(doCheckpoint = false, doChecksum = true)
  }

  test("DomainMetadata deletion [w/ checkpoint, w/ checksum]") {
    validateDeletionHelper(doCheckpoint = true, doChecksum = true)
  }

  test("Multiple DomainMetadatas with the same domain should fail in single transaction") {
    val table = "testTable"
    withTable(table) {
      sql(
        s"""
           | CREATE TABLE $table(id int) USING delta
           | tblproperties
           | ('${TableFeatureProtocolUtils.propertyKey(DomainMetadataTableFeature)}' = 'enabled')
           |""".stripMargin)
      (1 to 100).toDF("id").write.format("delta").mode("append").saveAsTable(table)
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(table))
      val domainMetadatas =
        DomainMetadata("testDomain1", Map.empty, false) ::
          DomainMetadata("testDomain1", Map.empty, false) :: Nil
      val e = intercept[DeltaIllegalArgumentException] {
        deltaLog.startTransaction().commit(domainMetadatas, Truncate())
      }
      assertEquals(e.getMessage,
        "Internal error: two DomainMetadata actions within the same transaction have " +
          "the same domain testDomain1")
    }
  }

  test("Validate the failure when table feature is not enabled") {
    withTempDir { dir =>
      (1 to 100).toDF().write.format("delta").save(dir.getAbsolutePath)
      val deltaLog = DeltaLog.forTable(spark, dir)
      val domainMetadatas = DomainMetadata("testDomain1", Map.empty, false) :: Nil
      val e = intercept[DeltaIllegalArgumentException] {
        deltaLog.startTransaction().commit(domainMetadatas, Truncate())
      }
      assertEquals(e.getMessage,
        "Detected DomainMetadata action(s) for domains [testDomain1], " +
          "but DomainMetadataTableFeature is not enabled.")
    }
  }
}
