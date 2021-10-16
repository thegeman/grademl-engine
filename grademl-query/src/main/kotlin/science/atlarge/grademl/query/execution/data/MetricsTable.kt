package science.atlarge.grademl.query.execution.data

import science.atlarge.grademl.core.GradeMLJob
import science.atlarge.grademl.core.TimestampNs
import science.atlarge.grademl.core.models.resource.Metric
import science.atlarge.grademl.core.models.resource.MetricDataIterator
import science.atlarge.grademl.query.analysis.ASTAnalysis
import science.atlarge.grademl.query.analysis.FilterConditionSeparation
import science.atlarge.grademl.query.execution.ExpressionEvaluation
import science.atlarge.grademl.query.execution.IndexedSortColumn
import science.atlarge.grademl.query.execution.SortColumn
import science.atlarge.grademl.query.execution.scanners.FilteringScanner
import science.atlarge.grademl.query.execution.scanners.RemappingScanner
import science.atlarge.grademl.query.execution.scanners.SortingScanner
import science.atlarge.grademl.query.language.*
import science.atlarge.grademl.query.model.*

class MetricsTable private constructor(
    private val gradeMLJob: GradeMLJob,
    private val selectedColumnIds: List<Int>,
    private val filterCondition: Expression?,
    private val sortColumns: List<IndexedSortColumn>
) : Table {

    override val columns = selectedColumnIds.map { i -> COLUMNS[i] }

    override val columnsOptimizedForFilter = columns.filter { it.isStatic }

    override val columnsOptimizedForSort
        get() = columnsOptimizedForFilter

    private val filterConditionOnMetricRows: Expression?
    private val filterConditionOnDataRows: Expression?

    init {
        // Split the filter condition into terms and determine which can be computed from static columns
        if (filterCondition == null) {
            filterConditionOnMetricRows = null
            filterConditionOnDataRows = null
        } else {
            val separationResult = FilterConditionSeparation.splitFilterConditionByColumns(
                filterCondition, listOf(COLUMNS.indices.filter { COLUMNS[it].isStatic }.toSet())
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
        val staticSortColumns = sortColumns.takeWhile { COLUMNS[it.columnIndex].isStatic }
        for (sortColumn in staticSortColumns.asReversed()) {
            when (sortColumn.columnIndex) {
                COLUMN_CAPACITY -> {
                    if (sortColumn.ascending) metrics.sortBy { it.data.maxValue }
                    else metrics.sortByDescending { it.data.maxValue }
                }
                COLUMN_PATH -> {
                    if (sortColumn.ascending) metrics.sortBy { it.path.asPlainPath }
                    else metrics.sortByDescending { it.path.asPlainPath }
                }
                COLUMN_TYPE -> {
                    if (sortColumn.ascending) metrics.sortBy { it.type.asPlainPath }
                    else metrics.sortByDescending { it.type.asPlainPath }
                }
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
        if (sortColumns.size > staticSortColumns.size) {
            // Drop columns if possible to sort fewer data elements
            val preSortNeededColumns = (sortColumns.map { it.columnIndex } + selectedColumnIds).distinct().sorted()
            if (preSortNeededColumns != scannerSelectedColumns) {
                scannerSelectedColumns = preSortNeededColumns
                scanner = RemappingScanner(scanner, preSortNeededColumns.map { originalColumnId ->
                    scannerSelectedColumns.indexOf(originalColumnId)
                })
            }
            // Determine which columns to sort by and which columns have already been sorted
            val remappedSortColumns = sortColumns.map { sortColumn ->
                IndexedSortColumn(scannerSelectedColumns.indexOf(sortColumn.columnIndex), sortColumn.ascending)
            }
            // Sort the input
            scanner = SortingScanner(
                scanner,
                scannerSelectedColumns.size,
                remappedSortColumns,
                remappedSortColumns.take(staticSortColumns.size)
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
        return MetricsTable(gradeMLJob, newSelectedColumnIds, filterCondition, sortColumns)
    }

    override fun filteredWith(condition: Expression): Table {
        val newFilterCondition = ASTAnalysis.analyzeExpression(
            if (filterCondition == null) condition else BinaryExpression(condition, filterCondition, BinaryOp.AND),
            COLUMNS
        )
        return MetricsTable(gradeMLJob, selectedColumnIds, newFilterCondition, sortColumns)
    }

    override fun sortedBy(sortColumns: List<SortColumn>): Table {
        val addedSortColumns = sortColumns.map { c ->
            val index = columns.indexOfFirst { it.name == c.column.columnName }
            require(index in selectedColumnIds.indices) { "Cannot sort by column that is not selected" }
            IndexedSortColumn(selectedColumnIds[index], c.ascending)
        }

        val combinedSortColumns = mutableListOf<IndexedSortColumn>()
        val usedColumnIds = mutableSetOf<Int>()
        for (c in addedSortColumns + this.sortColumns) {
            if (c.columnIndex !in usedColumnIds) {
                combinedSortColumns.add(c)
                usedColumnIds.add(c.columnIndex)
            }
        }

        return MetricsTable(gradeMLJob, selectedColumnIds, filterCondition, combinedSortColumns)
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
                    COLUMN_CAPACITY -> outValue.numericValue = metric.data.maxValue
                    COLUMN_PATH -> outValue.stringValue = metric.path.toString()
                    COLUMN_TYPE -> outValue.stringValue = metric.type.toString()
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
        const val COLUMN_START_TIME =  Column.INDEX_START_TIME
        const val COLUMN_END_TIME =    Column.INDEX_END_TIME
        const val COLUMN_DURATION =    Column.INDEX_DURATION
        const val COLUMN_UTILIZATION = Column.RESERVED_COLUMNS
        const val COLUMN_USAGE =       Column.RESERVED_COLUMNS + 1
        const val COLUMN_CAPACITY =    Column.RESERVED_COLUMNS + 2
        const val COLUMN_PATH =        Column.RESERVED_COLUMNS + 3
        const val COLUMN_TYPE =        Column.RESERVED_COLUMNS + 4

        val COLUMNS = listOf(
            Column.START_TIME,
            Column.END_TIME,
            Column.DURATION,
            Column("utilization", "utilization", COLUMN_UTILIZATION, Type.NUMERIC, false),
            Column("usage", "usage", COLUMN_USAGE, Type.NUMERIC, false),
            Column("capacity", "capacity", COLUMN_CAPACITY, Type.NUMERIC, true),
            Column("path", "path", COLUMN_PATH, Type.STRING, true),
            Column("type", "type", COLUMN_TYPE, Type.STRING, true)
        )
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
            MetricsTable.COLUMN_START_TIME ->
                outValue.numericValue = (dataIterator.currentStartTime - deltaTs) * (1 / 1e9)
            MetricsTable.COLUMN_END_TIME ->
                outValue.numericValue = (dataIterator.currentEndTime - deltaTs) * (1 / 1e9)
            MetricsTable.COLUMN_DURATION ->
                outValue.numericValue = (dataIterator.currentEndTime - dataIterator.currentStartTime) * (1 / 1e9)
            MetricsTable.COLUMN_UTILIZATION ->
                outValue.numericValue = dataIterator.currentValue / metric.data.maxValue
            MetricsTable.COLUMN_USAGE ->
                outValue.numericValue = dataIterator.currentValue
            MetricsTable.COLUMN_CAPACITY ->
                outValue.numericValue = metric.data.maxValue
            MetricsTable.COLUMN_PATH ->
                outValue.stringValue = metric.path.toString()
            MetricsTable.COLUMN_TYPE ->
                outValue.stringValue = metric.type.toString()
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
