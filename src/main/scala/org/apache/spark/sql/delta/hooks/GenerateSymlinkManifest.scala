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

package org.apache.spark.sql.delta.hooks

import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Concat, Expression, Literal, ScalaUDF}
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.execution.datasources.InMemoryFileIndex
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.SerializableConfiguration

trait GenerateSymlinkManifest extends PostCommitHook with DeltaLogging with Serializable {

  def manifestType(): ManifestType
  type T <: ManifestRawEntry
  /**
   * transform dataframe to the corresponding Dataset of ManifestRawEntry
   * @param ds
   * @return
   */
  protected def getManifestContent(ds: DataFrame): Dataset[T]
  protected def writeSingleManifestFile(manifestDirAbsPath: String,
                                        manifestRawEntries: Iterator[T],
                                        tableAbsPathForManifest: String,
                                        hadoopConf: SerializableConfiguration)

  lazy val manifestTypeSimpleName = manifestType().simpleName

  val RELATIVE_PARTITION_DIR_COL_NAME = "relativePartitionDir"
  val MANIFEST_FILE_NAME = "manifest"

  val CONFIG_NAME_ROOT = s"compatibility.symlinkFormatManifest.$manifestTypeSimpleName"
  val MANIFEST_LOCATION = s"_symlink_format_manifest_${manifestTypeSimpleName}"
  val OP_TYPE_ROOT = s"delta.compatibility.symlinkFormatManifest.$manifestTypeSimpleName"
  val FULL_MANIFEST_OP_TYPE = s"$OP_TYPE_ROOT.full"
  val INCREMENTAL_MANIFEST_OP_TYPE = s"$OP_TYPE_ROOT.incremental"

  override val name: String = s"Generate Symlink Format Manifest($manifestTypeSimpleName)"

  override def run(
      spark: SparkSession,
      txn: OptimisticTransactionImpl,
      committedActions: Seq[Action]): Unit = {
    generateIncrementalManifest(spark, txn.deltaLog, txn.snapshot, committedActions)
  }

  override def handleError(error: Throwable, version: Long): Unit = {
    throw DeltaErrors.postCommitHookFailedException(this, version, name, error)
  }

  /**
   * Generate manifest files incrementally, that is, only for the table partitions touched by the
   * given actions.
   */
  protected def generateIncrementalManifest(
      spark: SparkSession,
      deltaLog: DeltaLog,
      txnReadSnapshot: Snapshot,
      actions: Seq[Action]): Unit = recordManifestGeneration(deltaLog, full = false) {

    import spark.implicits._
    val currentSnapshot = deltaLog.snapshot

    val partitionCols = currentSnapshot.metadata.partitionColumns
    val manifestRootDirPath = new Path(deltaLog.dataPath, MANIFEST_LOCATION)
    val hadoopConf = new SerializableConfiguration(spark.sessionState.newHadoopConf())
    val fs = deltaLog.dataPath.getFileSystem(hadoopConf.value)
    if (!fs.exists(manifestRootDirPath)) {
      generateFullManifest(spark, deltaLog)
      return
    }

    // Find all the manifest partitions that need to updated or deleted
    val (allFilesInUpdatedPartitions, nowEmptyPartitions) = if (partitionCols.nonEmpty) {
      // Get the partitions where files were added
      val partitionsOfAddedFiles = actions.collect { case a: AddFile => a.partitionValues }.toSet

      // Get the partitions where files were deleted
      val removedFileNames =
        spark.createDataset(actions.collect { case r: RemoveFile => r.path }).toDF("path")
      val partitionValuesOfRemovedFiles =
        txnReadSnapshot.allFiles.join(removedFileNames, "path").select("partitionValues").persist()
      try {
        val partitionsOfRemovedFiles =
          partitionValuesOfRemovedFiles.as[Map[String, String]].collect().toSet

        // Get the files present in the updated partitions
        val partitionsUpdated: Set[Map[String, String]] =
          partitionsOfAddedFiles ++ partitionsOfRemovedFiles
        val filesInUpdatedPartitions = currentSnapshot.allFiles.filter { a =>
          partitionsUpdated.contains(a.partitionValues)
        }

        // Find the current partitions
        val currentPartitionRelativeDirs =
          withRelativePartitionDir(spark, partitionCols, currentSnapshot.allFiles)
            .select("relativePartitionDir").distinct()

        // Find the partitions that became empty and delete their manifests
        val partitionRelativeDirsOfRemovedFiles =
          withRelativePartitionDir(spark, partitionCols, partitionValuesOfRemovedFiles)
            .select("relativePartitionDir").distinct()

        val partitionsThatBecameEmpty =
          partitionRelativeDirsOfRemovedFiles.join(
            currentPartitionRelativeDirs, Seq("relativePartitionDir"), "leftanti")
            .as[String].collect()

        (filesInUpdatedPartitions, partitionsThatBecameEmpty)
      } finally {
        partitionValuesOfRemovedFiles.unpersist()
      }
    } else {
      (currentSnapshot.allFiles, Array.empty[String])
    }

    val manifestFilePartitionsWritten = writeManifestFiles(
      deltaLog.dataPath,
      manifestRootDirPath.toString,
      allFilesInUpdatedPartitions,
      partitionCols,
      hadoopConf)

    if (nowEmptyPartitions.nonEmpty) {
      deleteManifestFiles(manifestRootDirPath.toString, nowEmptyPartitions, hadoopConf)
    }

    // Post stats
    val stats = SymlinkManifestStats(
      filesWritten = manifestFilePartitionsWritten.size,
      filesDeleted = nowEmptyPartitions.length,
      partitioned = partitionCols.nonEmpty)
    recordDeltaEvent(deltaLog, s"$INCREMENTAL_MANIFEST_OP_TYPE.stats", data = stats)
  }

  /**
   * Generate manifest files for all the partitions in the table. Note, this will ensure that
   * that stale and unnecessary files will be vacuumed.
   */
  def generateFullManifest(
      spark: SparkSession,
      deltaLog: DeltaLog): Unit = recordManifestGeneration(deltaLog, full = true) {

    val snapshot = deltaLog.update(stalenessAcceptable = false)
    val partitionCols = snapshot.metadata.partitionColumns
    val manifestRootDirPath = new Path(deltaLog.dataPath, MANIFEST_LOCATION).toString
    val hadoopConf = new SerializableConfiguration(spark.sessionState.newHadoopConf())

    // Update manifest files of the current partitions
    val newManifestPartitionRelativePaths = writeManifestFiles(
      deltaLog.dataPath,
      manifestRootDirPath,
      snapshot.allFiles,
      partitionCols,
      hadoopConf)

    // Get the existing manifest files as relative partition paths, that is,
    // [ "col1=0/col2=0", "col1=1/col2=1", "col1=2/col2=2" ]
    val fs = deltaLog.dataPath.getFileSystem(hadoopConf.value)
    val existingManifestPartitionRelativePaths = {
      val manifestRootDirAbsPath = fs.makeQualified(new Path(manifestRootDirPath))
      if (fs.exists(manifestRootDirAbsPath)) {
        val index = new InMemoryFileIndex(spark, Seq(manifestRootDirAbsPath), Map.empty, None)
        val prefixToStrip = manifestRootDirAbsPath.toUri.getPath
        index.inputFiles.map { p =>
          // Remove root directory "rootDir" path from the manifest file paths like
          // "rootDir/col1=0/col2=0/manifest" to get the relative partition dir "col1=0/col2=0".
          // Note: It important to compare only the "path" in the URI and not the user info in it.
          // In s3a://access-key:secret-key@host/path, the access-key and secret-key may change
          // unknowingly to `\` and `%` encoding between the root dir and file names generated
          // by listing.
          val relativeManifestFilePath =
            new Path(p).toUri.getPath.stripPrefix(prefixToStrip).stripPrefix(Path.SEPARATOR)
          new Path(relativeManifestFilePath).getParent.toString // returns "col1=0/col2=0"
        }.filterNot(_.trim.isEmpty).toSet
      } else Set.empty[String]
    }

    // Delete manifest files for partitions that are not in current and so weren't overwritten
    val manifestFilePartitionsToDelete =
      existingManifestPartitionRelativePaths.diff(newManifestPartitionRelativePaths)
    deleteManifestFiles(manifestRootDirPath, manifestFilePartitionsToDelete, hadoopConf)

    // Post stats
    val stats = SymlinkManifestStats(
      filesWritten = newManifestPartitionRelativePaths.size,
      filesDeleted = manifestFilePartitionsToDelete.size,
      partitioned = partitionCols.nonEmpty)
    recordDeltaEvent(deltaLog, s"$FULL_MANIFEST_OP_TYPE.stats", data = stats)
  }

  /**
   * Write the manifest files and return the partition relative paths of the manifests written.
   *
   * @param deltaLogDataPath     path of the table data (e.g., tablePath which has _delta_log in it)
   * @param manifestRootDirPath  root directory of the manifest files (e.g., tablePath/_manifest/)
   * @param fileNamesForManifest relative paths or file names of data files for being written into
   *                             the manifest (e.g., partition=1/xyz.parquet)
   * @param partitionCols        Table partition columns
   * @param hadoopConf           Hadoop configuration to use
   * @return Set of partition relative paths of the written manifest files (e.g., part1=1/part2=2)
   */
  protected def writeManifestFiles(
                                    deltaLogDataPath: Path,
                                    manifestRootDirPath: String,
                                    fileNamesForManifest: Dataset[AddFile],
                                    partitionCols: Seq[String],
                                    hadoopConf: SerializableConfiguration): Set[String] = {
    val spark = fileNamesForManifest.sparkSession
    import spark.implicits._

    val tableAbsPathForManifest =
      LogStore(spark.sparkContext).resolvePathOnPhysicalStorage(deltaLogDataPath).toString

    val updatedPartitionRelativePaths =
      if (fileNamesForManifest.isEmpty && partitionCols.isEmpty) {
        writeSingleManifestFile(manifestRootDirPath, Iterator(),
          tableAbsPathForManifest, hadoopConf)
        Set.empty[String]
      } else {
        withRelativePartitionDir(spark, partitionCols, fileNamesForManifest)
          .transform(getManifestContent)
          .groupByKey(_.relativePartitionDir)
          .mapGroups {
            (relativePartitionDir: String, manifestEntries: Iterator[T]) =>
            val manifestPartitionDirAbsPath = {
              if (relativePartitionDir == null || relativePartitionDir.isEmpty) manifestRootDirPath
              else new Path(manifestRootDirPath, relativePartitionDir).toString
            }

            writeSingleManifestFile(manifestPartitionDirAbsPath, manifestEntries,
              tableAbsPathForManifest, hadoopConf)

            relativePartitionDir
          }.collect().toSet
      }

    logInfo(s"Generated manifest partitions for $deltaLogDataPath " +
      s"[${updatedPartitionRelativePaths.size}]:\n\t" +
      updatedPartitionRelativePaths.mkString("\n\t"))

    updatedPartitionRelativePaths
  }

  /**
   * Delete manifest files in the given paths.
   *
   * @param manifestRootDirPath root directory of the manifest files (e.g., tablePath/_manifest/)
   * @param partitionRelativePathsToDelete partitions to delete manifest files from
   *                                       (e.g., part1=1/part2=2/)
   * @param hadoopConf Hadoop configuration to use
   */
  private def deleteManifestFiles(
      manifestRootDirPath: String,
      partitionRelativePathsToDelete: Iterable[String],
      hadoopConf: SerializableConfiguration): Unit = {

    val fs = new Path(manifestRootDirPath).getFileSystem(hadoopConf.value)
    partitionRelativePathsToDelete.foreach { path =>
      val absPathToDelete = new Path(manifestRootDirPath, path)
      fs.delete(absPathToDelete, true)
    }

    logInfo(s"Deleted manifest partitions [${partitionRelativePathsToDelete.size}]:\n\t" +
      partitionRelativePathsToDelete.mkString("\n\t"))
  }

  /**
   * Append a column `relativePartitionDir` to the given Dataset which has `partitionValues` as
   * one of the columns. `partitionValues` is a map-type column that contains values of the
   * given `partitionCols`.
   */
  protected def withRelativePartitionDir(
      spark: SparkSession,
      partitionCols: Seq[String],
      datasetWithPartitionValues: Dataset[_]) = {

    require(datasetWithPartitionValues.schema.fieldNames.contains("partitionValues"))
    val colNamePrefix = "_col_"

    // Flatten out nested partition value columns while renaming them, so that the new columns do
    // not conflict with existing columns in DF `pathsWithPartitionValues.
    val colToRenamedCols = partitionCols.map { column => column -> s"$colNamePrefix$column" }

    val df = colToRenamedCols.foldLeft(datasetWithPartitionValues.toDF()) {
      case(currentDs, (column, renamedColumn)) =>
        currentDs.withColumn(renamedColumn, col(s"partitionValues.`$column`"))
    }

    // Mapping between original column names to use for generating partition path and
    // attributes referring to corresponding columns added to DF `pathsWithPartitionValues`.
    val colNameToAttribs =
      colToRenamedCols.map { case (col, renamed) => col -> UnresolvedAttribute.quoted(renamed) }

    // Build an expression that can generate the path fragment col1=value/col2=value/ from the
    // partition columns. Note: The session time zone maybe different from the time zone that was
    // used to write the partition structure of the actual data files. This may lead to
    // inconsistencies between the partition structure of metadata files and data files.
    val relativePartitionDirExpression = generatePartitionPathExpression(
      colNameToAttribs,
      spark.sessionState.conf.sessionLocalTimeZone)

    df.withColumn(RELATIVE_PARTITION_DIR_COL_NAME, new Column(relativePartitionDirExpression))
      .drop(colToRenamedCols.map(_._2): _*)
  }

  /** Expression that given partition columns builds a path string like: col1=val/col2=val/... */
  protected def generatePartitionPathExpression(
      partitionColNameToAttrib: Seq[(String, Attribute)],
      timeZoneId: String): Expression = Concat(
    partitionColNameToAttrib.zipWithIndex.flatMap { case ((colName, col), i) =>
      val partitionName = ScalaUDF(
        ExternalCatalogUtils.getPartitionPathString _,
        StringType,
        Seq(Literal(colName), Cast(col, StringType, Option(timeZoneId))),
        Seq(true, true))
      if (i == 0) Seq(partitionName) else Seq(Literal(Path.SEPARATOR), partitionName)
    }
  )

  protected def createManifestDir(manifestDirAbsPath: String,
                                  hadoopConf: SerializableConfiguration): Path = {
    val manifestFilePath = new Path(manifestDirAbsPath, MANIFEST_FILE_NAME)
    val fs = manifestFilePath.getFileSystem(hadoopConf.value)
    fs.mkdirs(manifestFilePath.getParent)

    manifestFilePath
  }

  private def recordManifestGeneration(deltaLog: DeltaLog, full: Boolean)(thunk: => Unit): Unit = {
    val (opType, manifestType) =
      if (full) FULL_MANIFEST_OP_TYPE -> "full"
      else INCREMENTAL_MANIFEST_OP_TYPE -> "incremental"
    recordDeltaOperation(deltaLog, opType) {
      withStatusCode("DELTA", s"Updating $manifestType Hive manifest for the Delta table") {
        thunk
      }
    }
  }


  case class SymlinkManifestStats(
      filesWritten: Int,
      filesDeleted: Int,
      partitioned: Boolean)
}

sealed trait ManifestType {
  val mode: String
  val simpleName: String
}
case object PrestoManifestType extends ManifestType {
  override val mode: String = "symlink_format_manifest"
  override val simpleName: String = "presto"
}
case object RedshiftManifestType extends ManifestType {
  override val mode: String = "redshift_format_manifest"
  override val simpleName: String = "redshift"
}

/**
 * the raw columns information to build the specific type of manifest entry
 */
trait ManifestRawEntry extends Product {
  val relativePartitionDir: String
  val path: String
}

