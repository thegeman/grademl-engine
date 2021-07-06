package science.atlarge.grademl.query.execution.data

import science.atlarge.grademl.core.GradeMLJob
import science.atlarge.grademl.core.TimestampNs
import science.atlarge.grademl.core.models.resource.Metric
import science.atlarge.grademl.core.models.resource.MetricDataIterator
import science.atlarge.grademl.query.analysis.ASTAnalysis
import science.atlarge.grademl.query.analysis.FilterConditionSeparation
import science.atlarge.grademl.query.execution.ExpressionEvaluation
import science.atlarge.grademl.query.execution.impl.FilteringScanner
import science.atlarge.grademl.query.execution.impl.RemappingScanner
import science.atlarge.grademl.query.execution.impl.SortingScanner
import science.atlarge.grademl.query.language.*
import science.atlarge.grademl.query.model.*

class MetricsTable private constructor(
    private val gradeMLJob: GradeMLJob,
    private val selectedColumnIds: List<Int>,
    private val filterCondition: Expression?,
    private val sortColumnIds: List<Int>
) : Table {

    override val columns = selectedColumnIds.map { i -> COLUMNS[i] }

    override val columnsOptimizedForFilter = columns.filter { it in STATIC_COLUMNS }

    override val columnsOptimizedForSort
        get() = columnsOptimizedForFilter

    override val isGrouped = false

    private val filterConditionOnMetricRows: Expression?
    private val filterConditionOnDataRows: Expression?

    init {
        // Split the filter condition into terms and determine which can be computed from static columns
        if (filterCondition == null) {
            filterConditionOnMetricRows = null
            filterConditionOnDataRows = null
        } else {
            val separationResult = FilterConditionSeparation.splitFilterConditionByColumns(
                filterCondition, listOf(COLUMNS.indices.filter { COLUMNS[it] in STATIC_COLUMNS }.toSet())
            )
            filterConditionOnMetricRows = separationResult.filterExpressionPerSplit[0]
            filterConditionOnDataRows = separationResult.remainingFilterExpression
        }
    }

    constructor(gradeMLJob: GradeMLJob) : this(gradeMLJob, COLUMNS.indices.toList(), null, emptyList())

    override fun scan(): RowScanner {
        // Get list of metrics after applying basic filter (if needed)
        val metrics = listMetricsMatchingFilter().toMutableList()
        // Sort list of metrics (if needed)
        val staticSortColumns = sortColumnIds.takeWhile { COLUMNS[it] in STATIC_COLUMNS }
        for (columnId in staticSortColumns.asReversed()) {
            when (columnId) {
                5 -> /* capacity */ metrics.sortBy { it.data.maxValue }
                6 -> /* path */ metrics.sortBy { it.path.asPlainPath }
                7 -> /* type */ metrics.sortBy { it.type.asPlainPath }
            }
        }

        // Create the basic table scanner
        var scanner: RowScanner = MetricsTableScanner(metrics, gradeMLJob.unifiedExecutionModel.rootPhase.startTime)
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
        return MetricsTable(gradeMLJob, newSelectedColumnIds, filterCondition, sortColumnIds)
    }

    override fun filteredWith(condition: Expression): Table {
        val newFilterCondition = ASTAnalysis.analyzeExpression(
            if (filterCondition == null) condition else BinaryExpression(condition, filterCondition, BinaryOp.AND),
            COLUMNS
        )
        return MetricsTable(gradeMLJob, selectedColumnIds, newFilterCondition, sortColumnIds)
    }

    override fun sortedBy(sortColumns: List<ColumnLiteral>): Table {
        val columnIds = sortColumns.map { c ->
            val index = columns.indexOfFirst { it.name == c.columnName }
            require(index in selectedColumnIds.indices)
            selectedColumnIds[index]
        }
        val newSortColumnIds = (columnIds + sortColumnIds).distinct()
        return MetricsTable(gradeMLJob, selectedColumnIds, filterCondition, newSortColumnIds)
    }

    private fun listMetricsMatchingFilter(): List<Metric> {
        val allMetrics = gradeMLJob.unifiedResourceModel.rootResource.metricsInTree.toList()
        if (filterConditionOnMetricRows == null) return allMetrics
        // Compute the filter condition for each metric
        val metricRowWrapper = object : Row {
            lateinit var metric: Metric
            override val columnCount = COLUMNS.size
            override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
                when (columnId) {
                    5 -> /* capacity */ outValue.numericValue = metric.data.maxValue
                    6 -> /* path */ outValue.stringValue = metric.path.toString()
                    7 -> /* type */ outValue.stringValue = metric.type.toString()
                    else -> throw IllegalArgumentException()
                }
                return outValue
            }
        }
        val filterResult = TypedValue()
        return allMetrics.filter { metric ->
            metricRowWrapper.metric = metric
            ExpressionEvaluation.evaluate(filterConditionOnMetricRows, metricRowWrapper, filterResult).booleanValue
        }
    }

    companion object {
        val COLUMNS = listOf(
            Column("start_time", "start_time", Type.NUMERIC),
            Column("end_time", "end_time", Type.NUMERIC),
            Column("duration", "duration", Type.NUMERIC),
            Column("utilization", "utilization", Type.NUMERIC),
            Column("usage", "usage", Type.NUMERIC),
            Column("capacity", "capacity", Type.NUMERIC),
            Column("path", "path", Type.STRING),
            Column("type", "type", Type.STRING)
        )

        private val STATIC_COLUMN_NAMES = setOf("path", "type", "capacity")

        private val STATIC_COLUMNS = COLUMNS.filter { it.name in STATIC_COLUMN_NAMES }.toSet()
    }

}

private class MetricsTableScanner(
    private val metrics: List<Metric>,
    deltaTs: TimestampNs
) : RowScanner() {

    private var currentMetricIndex = -1
    private val rowWrapper = MetricsTableRow(deltaTs)

    override fun fetchRow(): Row? {
        if (currentMetricIndex == -1) nextMetric()
        while (currentMetricIndex < metrics.size && !rowWrapper.dataIterator.hasNext) {
            nextMetric()
        }
        if (!rowWrapper.dataIterator.hasNext) return null
        rowWrapper.dataIterator.next()
        return rowWrapper
    }

    private fun nextMetric() {
        currentMetricIndex++
        if (currentMetricIndex < metrics.size) {
            rowWrapper.metric = metrics[currentMetricIndex]
            rowWrapper.dataIterator = rowWrapper.metric.data.iterator()
        }
    }

}

private class MetricsTableRow(
    val deltaTs: TimestampNs
) : Row {

    lateinit var metric: Metric
    lateinit var dataIterator: MetricDataIterator

    override val columnCount = MetricsTable.COLUMNS.size

    override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
        when (columnId) {
            0 -> /* start_time */ outValue.numericValue = (dataIterator.currentStartTime - deltaTs) * (1 / 1e9)
            1 -> /* end_time */ outValue.numericValue = (dataIterator.currentEndTime - deltaTs) * (1 / 1e9)
            2 -> /* duration */ outValue.numericValue =
                (dataIterator.currentEndTime - dataIterator.currentStartTime) * (1 / 1e9)
            3 -> /* utilization */ outValue.numericValue = dataIterator.currentValue / metric.data.maxValue
            4 -> /* usage */ outValue.numericValue = dataIterator.currentValue
            5 -> /* capacity */ outValue.numericValue = metric.data.maxValue
            6 -> /* path */ outValue.stringValue = metric.path.toString()
            7 -> /* type */ outValue.stringValue = metric.type.toString()
            else -> {
                require(columnId !in 0 until columnCount) {
                    "Mismatch between MetricsTableRow and MetricsTable.COLUMNS"
                }
                throw IndexOutOfBoundsException("Column $columnId does not exist: table has $columnCount columns")
            }
        }
        return outValue
    }

}
