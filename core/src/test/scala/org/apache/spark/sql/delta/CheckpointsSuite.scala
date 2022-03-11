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
import java.net.URI

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.LocalLogStore
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path, RawLocalFileSystem}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession


class CheckpointsSuite extends QueryTest
    with SharedSparkSession {

  protected override def sparkConf = {
    // Set the gs LogStore impl to `LocalLogStore` so that it will work with `FakeGCSFileSystem`.
    // The default one is `HDFSLogStore` which requires a `FileContext` but we don't have one.
    super.sparkConf.set("spark.delta.logStore.gs.impl", classOf[LocalLogStore].getName)
  }

  test("SC-86940: isGCSPath") {
    val conf = new Configuration()
    assert(Checkpoints.isGCSPath(conf, new Path("gs://foo/bar")))
    // Scheme is case insensitive
    assert(Checkpoints.isGCSPath(conf, new Path("Gs://foo/bar")))
    assert(Checkpoints.isGCSPath(conf, new Path("GS://foo/bar")))
    assert(Checkpoints.isGCSPath(conf, new Path("gS://foo/bar")))
    assert(!Checkpoints.isGCSPath(conf, new Path("non-gs://foo/bar")))
    assert(!Checkpoints.isGCSPath(conf, new Path("/foo")))
    // Set the default file system and verify we can detect it
    conf.set("fs.defaultFS", "gs://foo/")
    conf.set("fs.gs.impl", classOf[FakeGCSFileSystem].getName)
    conf.set("fs.gs.impl.disable.cache", "true")
    assert(Checkpoints.isGCSPath(conf, new Path("/foo")))
  }

  test("SC-86940: writing a GCS checkpoint should happen in a new thread") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      spark.range(1).write.format("delta").save(path)

      // Use `FakeGCSFileSystem` which will verify we write in a separate gcs thread.
      withSQLConf(
          "fs.gs.impl" -> classOf[FakeGCSFileSystem].getName,
          "fs.gs.impl.disable.cache" -> "true") {
        DeltaLog.clearCache()
        val gsPath = new Path(s"gs://${tempDir.getCanonicalPath}")
        val deltaLog = DeltaLog.forTable(spark, gsPath)
        deltaLog.checkpoint()
      }
    }
  }

  test("multipart checkpoints") {
     withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath

      withSQLConf(
        DeltaSQLConf.DELTA_CHECKPOINT_MAX_ACTIONS_PER_FILE.key -> "10",
        DeltaConfigs.CHECKPOINT_INTERVAL.defaultTablePropertyKey -> "1") {
        // 3 actions, protocol, metaData, and new file
        spark.range(1).repartition(1).write.format("delta").save(path)
        val deltaLog = DeltaLog.forTable(spark, path)

        // 4 total actions, 1 new file
        spark.range(1).repartition(1).write.format("delta").mode("append").save(path)

        assert(deltaLog.lastCheckpoint.isDefined)
        deltaLog.lastCheckpoint.foreach { checkpointMetadata =>
          assert(checkpointMetadata.version == 1)
          assert(checkpointMetadata.parts.isEmpty)
        }
        val checkpointPath =
          FileNames.checkpointFileSingular(deltaLog.logPath, deltaLog.snapshot.version).toUri
        assert(new File(checkpointPath).exists())

        // 11 total actions, 7 new files
        spark.range(30).repartition(7).write.format("delta").mode("append").save(path)
        deltaLog.lastCheckpoint.foreach { checkpointMetadata =>
          assert(checkpointMetadata.version == 2)
          assert(checkpointMetadata.parts.isDefined)
          assert(checkpointMetadata.parts.get == 2)
        }

        var checkpointPaths =
          FileNames.checkpointFileWithParts(deltaLog.logPath, deltaLog.snapshot.version, 2)
        checkpointPaths.foreach(p => assert(new File(p.toUri).exists()))

        // 20 total actions, 9 new files
        spark.range(100).repartition(9).write.format("delta").mode("append").save(path)
        deltaLog.lastCheckpoint.foreach { checkpointMetadata =>
          assert(checkpointMetadata.version == 3)
          assert(checkpointMetadata.parts.isDefined)
          assert(checkpointMetadata.parts.get == 2)
        }
        assert(deltaLog.snapshot.version == 3)
        checkpointPaths =
          FileNames.checkpointFileWithParts(deltaLog.logPath, deltaLog.snapshot.version, 2)
        checkpointPaths.foreach(p => assert(new File(p.toUri).exists()))

        // 31 total actions, 11 new files
        spark.range(100).repartition(11).write.format("delta").mode("append").save(path)
        deltaLog.lastCheckpoint.foreach { checkpointMetadata =>
          assert(checkpointMetadata.version == 4)
          assert(checkpointMetadata.parts.isDefined)
          assert(checkpointMetadata.parts.get == 4)
        }
        assert(deltaLog.snapshot.version == 4)
        checkpointPaths =
          FileNames.checkpointFileWithParts(deltaLog.logPath, deltaLog.snapshot.version, 4)
        checkpointPaths.foreach(p => assert(new File(p.toUri).exists()))
      }

      // Increase max actions
      withSQLConf(DeltaSQLConf.DELTA_CHECKPOINT_MAX_ACTIONS_PER_FILE.key -> "100") {
        val deltaLog = DeltaLog.forTable(spark, path)
        // 100 total actions, 69 new files
        spark.range(1000).repartition(69).write.format("delta").mode("append").save(path)
        deltaLog.lastCheckpoint.foreach { checkpointMetadata =>
          assert(checkpointMetadata.version == 5)
          assert(checkpointMetadata.parts.isEmpty)
        }
        val checkpointPath =
          FileNames.checkpointFileSingular(deltaLog.logPath, deltaLog.snapshot.version).toUri
        assert(new File(checkpointPath).exists())

        // 101 total actions, 1 new file
        spark.range(1).repartition(1).write.format("delta").mode("append").save(path)
        deltaLog.lastCheckpoint.foreach { checkpointMetadata =>
          assert(checkpointMetadata.version == 6)
          assert(checkpointMetadata.parts.isDefined)
          assert(checkpointMetadata.parts.get == 2)
        }
         var checkpointPaths =
          FileNames.checkpointFileWithParts(deltaLog.logPath, deltaLog.snapshot.version, 2)
        checkpointPaths.foreach(p => assert(new File(p.toUri).exists()))
      }
    }
  }
}

/**
 * A fake GCS file system to verify delta commits are written in a separate gcs thread.
 */
class FakeGCSFileSystem extends RawLocalFileSystem {
  override def getScheme: String = "gs"
  override def getUri: URI = URI.create("gs:/")

  private def assertGCSThread(f: Path): Unit = {
    if (f.getName.contains(".json") || f.getName.contains(".checkpoint")) {
      assert(
        Thread.currentThread().getName.contains("delta-gcs-"),
        s"writing $f was happening in non gcs thread: ${Thread.currentThread()}")
    }
  }

  override def create(
      f: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {
    assertGCSThread(f)
    super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress)
  }

  override def create(
      f: Path,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {
    assertGCSThread(f)
    super.create(f, overwrite, bufferSize, replication, blockSize, progress)
  }
}

