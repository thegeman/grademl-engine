package science.atlarge.grademl.query.execution.data

import science.atlarge.grademl.core.GradeMLJob
import science.atlarge.grademl.core.TimestampNs
import science.atlarge.grademl.core.attribution.AttributedResourceData
import science.atlarge.grademl.core.models.execution.ExecutionPhase
import science.atlarge.grademl.core.models.resource.Metric
import science.atlarge.grademl.core.models.resource.MetricDataIterator
import science.atlarge.grademl.query.analysis.ASTAnalysis
import science.atlarge.grademl.query.analysis.FilterConditionSeparation
import science.atlarge.grademl.query.execution.ExpressionEvaluation
import science.atlarge.grademl.query.execution.scanners.FilteringScanner
import science.atlarge.grademl.query.execution.scanners.RemappingScanner
import science.atlarge.grademl.query.execution.scanners.SortingScanner
import science.atlarge.grademl.query.language.*
import science.atlarge.grademl.query.model.*

class AttributedMetricsTable private constructor(
    private val gradeMLJob: GradeMLJob,
    private val selectedColumnIds: List<Int>,
    private val filterCondition: Expression?,
    private val sortColumnIds: List<Int>
) : Table {

    override val columns = selectedColumnIds.map { i -> COLUMNS[i] }

    override val columnsOptimizedForFilter = columns.filter { it in STATIC_COLUMNS }

    override val columnsOptimizedForSort
        get() = columnsOptimizedForFilter

    private val filterConditionOnStaticColumns: Expression?
    private val filterConditionOnDataRows: Expression?

    init {
        // Split the filter condition into terms and determine which can be computed from static columns
        if (filterCondition == null) {
            filterConditionOnStaticColumns = null
            filterConditionOnDataRows = null
        } else {
            val separationResult = FilterConditionSeparation.splitFilterConditionByColumns(
                filterCondition, listOf(COLUMNS.indices.filter { COLUMNS[it] in STATIC_COLUMNS }.toSet())
            )
            filterConditionOnStaticColumns = separationResult.filterExpressionPerSplit[0]
            filterConditionOnDataRows = separationResult.remainingFilterExpression
        }
    }

    constructor(gradeMLJob: GradeMLJob) : this(gradeMLJob, COLUMNS.indices.toList(), null, emptyList())

    override fun scan(): RowScanner {
        // Get list of metric-phase pairs after applying basic filter (if needed)
        val metricPhasePairs = listMetricPhasePairsMatchingFilter().toMutableList()
        // Sort list of metric-phase pairs (if needed)
        val staticSortColumns = sortColumnIds.takeWhile { COLUMNS[it] in STATIC_COLUMNS }
        for (columnId in staticSortColumns.asReversed()) {
            when (columnId) {
                6 -> /* metric_path */ metricPhasePairs.sortBy { it.first.path.asPlainPath }
                7 -> /* metric_type */ metricPhasePairs.sortBy { it.first.type.asPlainPath }
                8 -> /* phase_path */ metricPhasePairs.sortBy { it.second.path }
                9 -> /* phase_type */ metricPhasePairs.sortBy { it.second.type.path }
            }
        }

        // Create the basic table scanner
        var scanner: RowScanner = AttributedMetricsTableScanner(
            metricPhasePairs,
            { metric, phase ->
                gradeMLJob.resourceAttribution.attributeMetricToPhase(metric, phase) as? AttributedResourceData
            },
            gradeMLJob.unifiedExecutionModel.rootPhase.startTime
        )
        var scannerSelectedColumns = COLUMNS.indices.toList()

        // Append the filter operation
        if (filterConditionOnDataRows != null) {
            val filterResult = TypedValue()
            scanner = FilteringScanner(scanner) { row ->
                ExpressionEvaluation.evaluate(filterConditionOnDataRows, row, filterResult).booleanValue
            }
        }

        // Append the sort operation
        if (sortColumnIds.size > staticSortColumns.size) {
            // Drop columns if possible to sort fewer data elements
            val preSortNeededColumns = (sortColumnIds + selectedColumnIds).distinct().sorted()
            if (preSortNeededColumns != scannerSelectedColumns) {
                scannerSelectedColumns = preSortNeededColumns
                scanner = RemappingScanner(scanner, preSortNeededColumns.map { originalColumnId ->
                    scannerSelectedColumns.indexOf(originalColumnId)
                })
            }
            // Determine which columns to sort by and which columns have already been sorted
            val remappedColumns = sortColumnIds.map { originalColumnId ->
                scannerSelectedColumns.indexOf(originalColumnId)
            }
            // Sort the input
            scanner = SortingScanner(
                scanner,
                scannerSelectedColumns.size,
                remappedColumns,
                remappedColumns.take(staticSortColumns.size)
            )
        }

        // Append the select operation
        if (selectedColumnIds != scannerSelectedColumns) {
            val remappedColumns = selectedColumnIds.map { originalColumnId ->
                scannerSelectedColumns.indexOf(originalColumnId)
            }
            scanner = RemappingScanner(scanner, remappedColumns)
        }

        // Return the compound scanner
        return scanner
    }

    override fun withSubsetColumns(subsetColumns: List<ColumnLiteral>): Table {
        val newSelectedColumnIds = subsetColumns.map { c ->
            val index = columns.indexOfFirst { it.name == c.columnName }
            require(index in selectedColumnIds.indices)
            selectedColumnIds[index]
        }
        return AttributedMetricsTable(gradeMLJob, newSelectedColumnIds, filterCondition, sortColumnIds)
    }

    override fun filteredWith(condition: Expression): Table {
        val newFilterCondition = ASTAnalysis.analyzeExpression(
            if (filterCondition == null) condition else BinaryExpression(condition, filterCondition, BinaryOp.AND),
            COLUMNS
        )
        return AttributedMetricsTable(gradeMLJob, selectedColumnIds, newFilterCondition, sortColumnIds)
    }

    override fun sortedBy(sortColumns: List<ColumnLiteral>): Table {
        val columnIds = sortColumns.map { c ->
            val index = columns.indexOfFirst { it.name == c.columnName }
            require(index in selectedColumnIds.indices)
            selectedColumnIds[index]
        }
        val newSortColumnIds = (columnIds + sortColumnIds).distinct()
        return AttributedMetricsTable(gradeMLJob, selectedColumnIds, filterCondition, newSortColumnIds)
    }

    private fun listMetricPhasePairsMatchingFilter(): List<Pair<Metric, ExecutionPhase>> {
        val allMetricPhasePairs = gradeMLJob.resourceAttribution.metrics.flatMap { metric ->
            gradeMLJob.resourceAttribution.leafPhases.map { phase ->
                metric to phase
            }
        }
        if (filterConditionOnStaticColumns == null) return allMetricPhasePairs
        // Compute the filter condition for each metric-phase pair
        val metricPhaseRowWrapper = object : Row {
            lateinit var metric: Metric
            lateinit var phase: ExecutionPhase
            override val columnCount = MetricsTable.COLUMNS.size
            override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
                when (columnId) {
                    6 -> /* metric_path */ outValue.stringValue = metric.path.toString()
                    7 -> /* metric_type */ outValue.stringValue = metric.type.toString()
                    8 -> /* phase_path */ outValue.stringValue = phase.path.toString()
                    9 -> /* phase_type */ outValue.stringValue = phase.type.path.toString()
                    else -> throw IllegalArgumentException()
                }
                return outValue
            }
        }
        val filterResult = TypedValue()
        return allMetricPhasePairs.filter { (metric, phase) ->
            metricPhaseRowWrapper.metric = metric
            metricPhaseRowWrapper.phase = phase
            ExpressionEvaluation.evaluate(
                filterConditionOnStaticColumns, metricPhaseRowWrapper, filterResult
            ).booleanValue
        }
    }

    companion object {
        val COLUMNS = listOf(
            Column("start_time", "start_time", Type.NUMERIC, ColumnFunction.TIME_START),
            Column("end_time", "end_time", Type.NUMERIC, ColumnFunction.TIME_END),
            Column("duration", "duration", Type.NUMERIC, ColumnFunction.TIME_DURATION),
            Column("utilization", "utilization", Type.NUMERIC, ColumnFunction.OTHER),
            Column("usage", "usage", Type.NUMERIC, ColumnFunction.OTHER),
            Column("capacity", "capacity", Type.NUMERIC, ColumnFunction.OTHER),
            Column("metric_path", "metric_path", Type.STRING, ColumnFunction.OTHER),
            Column("metric_type", "metric_type", Type.STRING, ColumnFunction.OTHER),
            Column("phase_path", "phase_path", Type.STRING, ColumnFunction.OTHER),
            Column("phase_type", "phase_type", Type.STRING, ColumnFunction.OTHER)
        )

        private val STATIC_COLUMN_NAMES = setOf("metric_path", "metric_type", "phase_path", "phase_type")

        private val STATIC_COLUMNS = COLUMNS.filter { it.name in STATIC_COLUMN_NAMES }.toSet()
    }

}

private class AttributedMetricsTableScanner(
    private val metricPhasePairs: List<Pair<Metric, ExecutionPhase>>,
    private val resourceAttribution: (Metric, ExecutionPhase) -> AttributedResourceData?,
    deltaTs: TimestampNs
) : RowScanner() {

    private var currentMetricPhasePairIndex = -1
    private val rowWrapper = AttributedMetricsTableRow(deltaTs)

    override fun fetchRow(): Row? {
        if (currentMetricPhasePairIndex == -1) nextMetricPhasePair()
        while (currentMetricPhasePairIndex < metricPhasePairs.size && !rowWrapper.dataIterator.hasNext) {
            nextMetricPhasePair()
        }
        if (!rowWrapper.dataIterator.hasNext) return null
        rowWrapper.dataIterator.next()
        rowWrapper.capacityIterator.next()
        return rowWrapper
    }

    private fun nextMetricPhasePair() {
        currentMetricPhasePairIndex++
        while (currentMetricPhasePairIndex < metricPhasePairs.size) {
            val (metric, phase) = metricPhasePairs[currentMetricPhasePairIndex]
            val attributedData = resourceAttribution(metric, phase)

            if (attributedData == null) {
                currentMetricPhasePairIndex++
                continue
            }

            rowWrapper.metric = metric
            rowWrapper.phase = phase
            rowWrapper.dataIterator = attributedData.metricData.iterator()
            rowWrapper.capacityIterator = attributedData.availableCapacity.iterator()
            break
        }
    }

}

private class AttributedMetricsTableRow(
    val deltaTs: TimestampNs
) : Row {

    lateinit var metric: Metric
    lateinit var phase: ExecutionPhase
    lateinit var dataIterator: MetricDataIterator
    lateinit var capacityIterator: MetricDataIterator

    override val columnCount = AttributedMetricsTable.COLUMNS.size

    override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
        when (columnId) {
            0 -> /* start_time */ outValue.numericValue = (dataIterator.currentStartTime - deltaTs) * (1 / 1e9)
            1 -> /* end_time */ outValue.numericValue = (dataIterator.currentEndTime - deltaTs) * (1 / 1e9)
            2 -> /* duration */ outValue.numericValue =
                (dataIterator.currentEndTime - dataIterator.currentStartTime) * (1 / 1e9)
            3 -> /* utilization */ outValue.numericValue = dataIterator.currentValue / capacityIterator.currentValue
            4 -> /* usage */ outValue.numericValue = dataIterator.currentValue
            5 -> /* capacity */ outValue.numericValue = capacityIterator.currentValue
            6 -> /* metric_path */ outValue.stringValue = metric.path.toString()
            7 -> /* metric_type */ outValue.stringValue = metric.type.toString()
            8 -> /* phase_path */ outValue.stringValue = phase.path.toString()
            9 -> /* phase_type */ outValue.stringValue = phase.type.path.toString()
            else -> {
                require(columnId !in 0 until columnCount) {
                    "Mismatch between AttributedMetricsTableRow and AttributedMetricsTable.COLUMNS"
                }
                throw IndexOutOfBoundsException("Column $columnId does not exist: table has $columnCount columns")
            }
        }
        return outValue
    }

}