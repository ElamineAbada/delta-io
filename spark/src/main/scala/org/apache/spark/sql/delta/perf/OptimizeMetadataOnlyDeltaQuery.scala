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

package org.apache.spark.sql.delta.perf

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaTable, Snapshot}
import org.apache.spark.sql.delta.commands.DeletionVectorUtils.isTableDVFree
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.stats.DeltaScanGenerator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.sql.Date
import java.util.Locale

/** Optimize COUNT, MIN and MAX expressions on Delta tables.
 * This optimization is only applied when the following conditions are met:
 * - The MIN/MAX columns data type is supported by the optimization (ByteType, ShortType,
 * IntegerType, LongType, FloatType, DoubleType, DateType).
 * - Table has no deletion vectors, or query has no MIN/MAX expressions.
 * - COUNT has no DISTINCT.
 * - Query has no filters.
 * - Query has no GROUP BY.
 * Example of valid query: SELECT COUNT(*), MIN(id), MAX(id) FROM MyDeltaTable
 */
trait OptimizeMetadataOnlyDeltaQuery {
  def optimizeQueryWithMetadata(plan: LogicalPlan): LogicalPlan = {
    plan.transformUpWithSubqueries {
      case agg@AggregateDeltaTable(tahoeLogFileIndex) =>
        createLocalRelationPlan(agg, tahoeLogFileIndex)
    }
  }

  protected def getDeltaScanGenerator(index: TahoeLogFileIndex): DeltaScanGenerator

  private def createLocalRelationPlan(
    plan: Aggregate,
    tahoeLogFileIndex: TahoeLogFileIndex): LogicalPlan = {
    val rowCount = extractGlobalCount(tahoeLogFileIndex)

    if (rowCount.isDefined) {
      val aggColumnsNames = Set(extractMinMaxFieldNames(plan).map(_.toLowerCase(Locale.ROOT)) : _*)
      val columnStats = extractMinMaxFromDeltaLog(tahoeLogFileIndex, aggColumnsNames)

      def checkStatsExists(attrRef: AttributeReference): Boolean = {
        columnStats.contains(attrRef.name) &&
          // Avoid StructType, it is not supported by this optimization
          // Sanity check only. If reference is nested column it would be GetStructType
          // instead of AttributeReference
          attrRef.references.size == 1 &&
          attrRef.references.head.dataType != StructType
      }

      def convertValueIfRequired(attrRef: AttributeReference, value: Any): Any = {
        if (attrRef.dataType == DateType && value != null) {
          DateTimeUtils.fromJavaDate(value.asInstanceOf[Date])
        } else {
          value
        }
      }

      val rewrittenAggregationValues = plan.aggregateExpressions.collect {
        case Alias(AggregateExpression(
        Count(Seq(Literal(1, _))), Complete, false, None, _), _) =>
          rowCount.get
        case Alias(tps@ToPrettyString(AggregateExpression(
        Count(Seq(Literal(1, _))), Complete, false, None, _), _), _) =>
          tps.copy(child = Literal(rowCount.get)).eval()
        case Alias(AggregateExpression(
        Min(minReference: AttributeReference), Complete, false, None, _), _)
          if checkStatsExists(minReference) =>
          convertValueIfRequired(minReference, columnStats(minReference.name).min)
        case Alias(tps@ToPrettyString(AggregateExpression(
        Min(minReference: AttributeReference), Complete, false, None, _), _), _)
          if checkStatsExists(minReference) =>
            val v = columnStats(minReference.name).min
            tps.copy(child = Literal(v)).eval()
        case Alias(AggregateExpression(
        Max(maxReference: AttributeReference), Complete, false, None, _), _)
          if checkStatsExists(maxReference) =>
          convertValueIfRequired(maxReference, columnStats(maxReference.name).max)
        case Alias(tps@ToPrettyString(AggregateExpression(
        Max(maxReference: AttributeReference), Complete, false, None, _), _), _)
          if checkStatsExists(maxReference) =>
            val v = columnStats(maxReference.name).max
            tps.copy(child = Literal(v)).eval()
      }

      if (plan.aggregateExpressions.size == rewrittenAggregationValues.size) {
        val r = LocalRelation(
          plan.output,
          Seq(InternalRow.fromSeq(rewrittenAggregationValues)))
        r
      } else {
        plan
      }
    }
    else {
      plan
    }
  }

  private def extractMinMaxFieldNames(plan: Aggregate): Seq[String] = {
    plan.aggregateExpressions.collect {
      case Alias(AggregateExpression(
        Min(minReference: AttributeReference), _, _, _, _), _) =>
        minReference.name
      case Alias(AggregateExpression(
        Max(maxReference: AttributeReference), _, _, _, _), _) =>
        maxReference.name
      case Alias(ToPrettyString(AggregateExpression(
        Min(minReference: AttributeReference), _, _, _, _), _), _) =>
        minReference.name
      case Alias(ToPrettyString(AggregateExpression(
        Max(maxReference: AttributeReference), _, _, _, _), _), _) =>
        maxReference.name
    }
  }

  /** Return the number of rows in the table or `None` if we cannot calculate it from stats */
  private def extractGlobalCount(tahoeLogFileIndex: TahoeLogFileIndex): Option[Long] = {
    // account for deleted rows according to deletion vectors
    val dvCardinality = coalesce(col("deletionVector.cardinality"), lit(0))
    val numLogicalRecords = (col("stats.numRecords") - dvCardinality).as("numLogicalRecords")
    val row = getDeltaScanGenerator(tahoeLogFileIndex).filesWithStatsForScan(Nil)
      .agg(
        sum(numLogicalRecords),
        // Calculate the number of files missing `numRecords`
        count(when(col("stats.numRecords").isNull, 1)))
      .first

    // The count agg is never null. A non-zero value means we have incomplete stats; otherwise,
    // the sum agg is either null (for an empty table) or gives an accurate record count.
    if (row.getLong(1) > 0) return None
    val numRecords = if (row.isNullAt(0)) 0 else row.getLong(0)
    Some(numRecords)
  }

  /**
   * Min and max values from Delta Log stats or partitionValues.
  */
  case class DeltaColumnStat(min: Any, max: Any)

  private def extractMinMaxFromStats(
    deltaScanGenerator: DeltaScanGenerator,
    lowerCaseColumnNames: Set[String]): Map[String, DeltaColumnStat] = {

    // TODO Update this to work with DV (https://github.com/delta-io/delta/issues/1485)
    val snapshot = deltaScanGenerator.snapshotToScan
    val dataColumns = snapshot.statCollectionPhysicalSchema.filter(col =>
      AggregateDeltaTable.isSupportedDataType(col.dataType) &&
      lowerCaseColumnNames.contains(col.name.toLowerCase(Locale.ROOT)))

    // Validate all the files has stats
    lazy val filesStatsCount = deltaScanGenerator.filesWithStatsForScan(Nil).select(
      count(when(col("stats.numRecords").isNull, 1)).as("missingNumRecords"),
      count(when(col("stats.numRecords") > 0, 1)).as("countNonEmptyFiles")).head

    lazy val allRecordsHasStats = filesStatsCount.getAs[Long]("missingNumRecords") == 0
    lazy val numFiles: Long = filesStatsCount.getAs[Long]("countNonEmptyFiles")

    // DELETE operations creates AddFile records with 0 rows, and no column stats.
    // We can safely ignore it since there is no data.
    lazy val files = deltaScanGenerator.filesWithStatsForScan(Nil)
      .filter(col("stats.numRecords") > 0)
    lazy val statsMinMaxNullColumns = files.select(col("stats.*"))
    if (dataColumns.isEmpty
      || !isTableDVFree(snapshot)
      || !allRecordsHasStats
      || numFiles == 0
      || !statsMinMaxNullColumns.columns.contains("minValues")
      || !statsMinMaxNullColumns.columns.contains("maxValues")
      || !statsMinMaxNullColumns.columns.contains("nullCount")) {
      Map.empty
    } else {
      // dataColumns can contain columns without stats if dataSkippingNumIndexedCols
      // has been increased
      val columnsWithStats = files.select(
        col("stats.minValues.*"),
        col("stats.maxValues.*"),
        col("stats.nullCount.*"))
        .columns.groupBy(identity).mapValues(_.size)
        .filter(x => x._2 == 3) // 3: minValues, maxValues, nullCount
        .map(x => x._1).toSet

      // Creates a tuple with physical name to avoid recalculating it multiple times
      val dataColumnsWithStats = dataColumns.map(x => (x, DeltaColumnMapping.getPhysicalName(x)))
        .filter(x => columnsWithStats.contains(x._2))

      val columnsToQuery = dataColumnsWithStats.flatMap { columnAndPhysicalName =>
        val dataType = columnAndPhysicalName._1.dataType
        val physicalName = columnAndPhysicalName._2

        Seq(col(s"stats.minValues.`$physicalName`").cast(dataType).as(s"min.$physicalName"),
          col(s"stats.maxValues.`$physicalName`").cast(dataType).as(s"max.$physicalName"),
          col(s"stats.nullCount.`$physicalName`").as(s"nullCount.$physicalName"))
      } ++ Seq(col(s"stats.numRecords").as(s"numRecords"))

      val minMaxExpr = dataColumnsWithStats.flatMap { columnAndPhysicalName =>
        val physicalName = columnAndPhysicalName._2

        // To validate if the column has stats we do two validation:
        // 1-) COUNT(nullCount.columnName) should be equals to numFiles,
        // since nullCount is always non-null.
        // 2-) The number of files with non-null min/max:
        // a. count(min.columnName)|count(max.columnName) +
        // the number of files where all rows are NULL:
        // b. count of (ISNULL(min.columnName) and nullCount.columnName == numRecords)
        // should be equals to numFiles
        Seq(
          s"""case when $numFiles = count(`nullCount.$physicalName`)
              | AND $numFiles = (count(`min.$physicalName`) + sum(case when
              |  ISNULL(`min.$physicalName`) and `nullCount.$physicalName` = numRecords
              |   then 1 else 0 end))
              | AND $numFiles = (count(`max.$physicalName`) + sum(case when
              |  ISNULL(`max.$physicalName`) AND `nullCount.$physicalName` = numRecords
              |   then 1 else 0 end))
              | then TRUE else FALSE end as `complete_$physicalName`""".stripMargin,
          s"min(`min.$physicalName`) as `min_$physicalName`",
          s"max(`max.$physicalName`) as `max_$physicalName`")
      }

      val statsResults = files.select(columnsToQuery: _*).selectExpr(minMaxExpr: _*).head

      dataColumnsWithStats
        .filter(x => statsResults.getAs[Boolean](s"complete_${x._2}"))
        .map { columnAndPhysicalName =>
          val column = columnAndPhysicalName._1
          val physicalName = columnAndPhysicalName._2
          column.name ->
            DeltaColumnStat(
              statsResults.getAs(s"min_$physicalName"),
              statsResults.getAs(s"max_$physicalName"))
        }.toMap
    }
  }

  private def extractMinMaxFromPartitionValue(
    snapshot: Snapshot,
    lowerCaseColumnNames: Set[String]): Map[String, DeltaColumnStat] = {

    val partitionedColumns = snapshot.metadata.partitionSchema
      .filter(col => AggregateDeltaTable.isSupportedDataType(col.dataType) &&
        lowerCaseColumnNames.contains(col.name.toLowerCase(Locale.ROOT)))
      .map(col => (col, DeltaColumnMapping.getPhysicalName(col)))

    if (partitionedColumns.isEmpty) {
      Map.empty
    } else {
      val partitionedColumnsValues = partitionedColumns.map { partitionedColumn =>
        val physicalName = partitionedColumn._2
        col(s"partitionValues.`$physicalName`")
          .cast(partitionedColumn._1.dataType).as(physicalName)
      }

      val partitionedColumnsAgg = partitionedColumns.flatMap { partitionedColumn =>
        val physicalName = partitionedColumn._2

        Seq(min(s"`$physicalName`").as(s"min_$physicalName"),
          max(s"`$physicalName`").as(s"max_$physicalName"))
      }

      val partitionedColumnsQuery = snapshot.allFiles
        .select(partitionedColumnsValues: _*)
        .agg(partitionedColumnsAgg.head, partitionedColumnsAgg.tail: _*)
        .head()

      partitionedColumns.map { partitionedColumn =>
        val physicalName = partitionedColumn._2

        partitionedColumn._1.name ->
          DeltaColumnStat(
            partitionedColumnsQuery.getAs(s"min_$physicalName"),
            partitionedColumnsQuery.getAs(s"max_$physicalName"))
      }.toMap
    }
  }

  private def extractMinMaxFromDeltaLog(
    tahoeLogFileIndex: TahoeLogFileIndex,
    lowerCaseColumnNames: Set[String]):
  CaseInsensitiveMap[DeltaColumnStat] = {
    val deltaScanGenerator = getDeltaScanGenerator(tahoeLogFileIndex)
    val snapshot = deltaScanGenerator.snapshotToScan
    val columnFromStats = extractMinMaxFromStats(deltaScanGenerator, lowerCaseColumnNames)

    if(lowerCaseColumnNames.equals(columnFromStats.keySet)) {
      CaseInsensitiveMap(columnFromStats)
    } else {
    CaseInsensitiveMap(
      columnFromStats.++
      (extractMinMaxFromPartitionValue(snapshot, lowerCaseColumnNames)))
    }
  }

  object AggregateDeltaTable {

    /** Only data type that are stored in stats without any loss of precision are supported. */
    def isSupportedDataType(dataType: DataType): Boolean = {
      // DecimalType is not supported because not all the values are correctly stored
      // For example -99999999999999999999999999999999999999 in stats is -1e38
      (dataType.isInstanceOf[NumericType] && !dataType.isInstanceOf[DecimalType]) ||
      dataType.isInstanceOf[DateType]
    }

    def getAggFunctionOptimizable(aggExpr: AggregateExpression): Option[DeclarativeAggregate] = {
      aggExpr match {
        case AggregateExpression(
          c@Count(Seq(Literal(1, _))), Complete, false, None, _) =>
            Some(c)
        case AggregateExpression(
          min@Min(minExpr), Complete, false, None, _) if isSupportedDataType(minExpr.dataType) =>
            Some(min)
        case AggregateExpression(
          max@Max(maxExpr), Complete, false, None, _) if isSupportedDataType(maxExpr.dataType) =>
            Some(max)
        case _ => None
      }
    }

    private def isStatsOptimizable(aggExpr: Seq[Alias]): Boolean = aggExpr.forall {
      case Alias(aggExpr: AggregateExpression, _) => getAggFunctionOptimizable(aggExpr).isDefined
      case Alias(ToPrettyString(aggExpr: AggregateExpression, _), _) =>
        getAggFunctionOptimizable(aggExpr).isDefined
      case _ => false
    }

    private def fieldsAreAttributeReference(fields: Seq[NamedExpression]): Boolean = fields.forall {
      // Fields should be AttributeReference to avoid getting the incorrect column name
      // from stats when we create the Local Relation, example
      // SELECT MAX(Column2) FROM (SELECT Column1 AS Column2 FROM TableName)
      // the AggregateExpression contains a reference to Column2, instead of Column1
      case _: AttributeReference => true
      case _ => false
    }

    def unapply(plan: Aggregate): Option[TahoeLogFileIndex] = plan match {
      case Aggregate(
        Nil, // GROUP BY not supported
        aggExpr: Seq[Alias @unchecked], // Underlying type is not checked because of type erasure.
        // Alias type check is done in isStatsOptimizable.
        PhysicalOperation(fields, Nil, DeltaTable(fileIndex: TahoeLogFileIndex)))
          if fileIndex.partitionFilters.isEmpty &&
            fieldsAreAttributeReference(fields) &&
            isStatsOptimizable(aggExpr) => Some(fileIndex)
      case Aggregate(
        Nil,
        aggExpr: Seq[Alias @unchecked],
        // When all columns are selected, there are no Project/PhysicalOperation
        DeltaTable(fileIndex: TahoeLogFileIndex))
          if fileIndex.partitionFilters.isEmpty &&
            isStatsOptimizable(aggExpr) => Some(fileIndex)
      case _ => None
    }
  }
}
