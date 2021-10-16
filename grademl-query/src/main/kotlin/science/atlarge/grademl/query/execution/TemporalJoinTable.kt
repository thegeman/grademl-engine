package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.analysis.ASTAnalysis
import science.atlarge.grademl.query.analysis.FilterConditionSeparation
import science.atlarge.grademl.query.execution.scanners.FilteringScanner
import science.atlarge.grademl.query.execution.scanners.SortingScanner
import science.atlarge.grademl.query.execution.scanners.TemporalJoinScanner
import science.atlarge.grademl.query.execution.scanners.TemporalJoinScannerInput
import science.atlarge.grademl.query.language.ColumnLiteral
import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.*

class TemporalJoinTable private constructor(
    private val inputTables: List<Table>,
    private val inputStartTimeColumns: List<Column>,
    private val inputEndTimeColumns: List<Column>,
    private val filterConditionPerInput: List<Expression?>,
    private val globalFilterCondition: Expression?
) : Table {

    private val columnsPerInput = inputTables.map { inputTable ->
        // Drop reserved columns for start and end time, which will be replaced by the join operation
        inputTable.columns.filter { c -> c.path != "_start_time" && c.path != "_end_time" }
    }

    private val columnOffsets: List<Int> = run {
        val offsets = mutableListOf<Int>()
        offsets.add(2)
        for (i in 1 until columnsPerInput.size) {
            offsets.add(offsets[i - 1] + columnsPerInput[i - 1].size)
        }
        offsets
    }

    private val startTimeColumnIndices = inputStartTimeColumns.mapIndexed { index, column ->
        inputTables[index].columns.indexOf(column)
    }

    private val endTimeColumnIndices = inputEndTimeColumns.mapIndexed { index, column ->
        inputTables[index].columns.indexOf(column)
    }

    override val columns = listOf(
        Column.START_TIME,
        Column.END_TIME
    ) + columnsPerInput.flatten()

    override fun scan(): RowScanner {
        // Create an input for a temporal join scanner for every input table
        val joinInputs = inputTables.indices.map { createTemporalJoinInput(it) }
        // Construct a nested scanner from binary temporal joins
        var compoundScanner: RowScanner = TemporalJoinScanner(joinInputs[0], joinInputs[1])
        for (i in 2 until joinInputs.size) {
            val nextInput = joinInputs[i]
            val leftColumnCount = 2 + columnsPerInput.take(i).sumOf { it.size }
            compoundScanner = TemporalJoinScanner(
                TemporalJoinScannerInput(
                    compoundScanner, leftColumnCount, 0, 1, true
                ),
                nextInput
            )
        }
        // Apply the global filter
        if (globalFilterCondition != null) {
            val filter = globalFilterCondition
            val scratch = TypedValue()
            compoundScanner = FilteringScanner(compoundScanner) { row ->
                ExpressionEvaluation.evaluate(filter, row, scratch).booleanValue
            }
        }
        return compoundScanner
    }

    override fun filteredWith(condition: Expression): Table {
        val analyzedCondition = ASTAnalysis.analyzeExpression(condition, columns)
        val columnSplits = columnOffsets.mapIndexed { index, columnOffset ->
            (columnOffset until (columnOffset + columnsPerInput[index].size)).toSet()
        }
        val splitCondition = FilterConditionSeparation.splitFilterConditionByColumns(analyzedCondition, columnSplits)
        val newFilterConditions = filterConditionPerInput.mapIndexed { index, oldExpression ->
            val newExpression = splitCondition.filterExpressionPerSplit[index]
            FilterConditionSeparation.mergeExpressions(listOfNotNull(oldExpression, newExpression))
        }
        val newGlobalCondition = FilterConditionSeparation.mergeExpressions(
            listOfNotNull(globalFilterCondition, splitCondition.remainingFilterExpression)
        )
        return TemporalJoinTable(
            inputTables, inputStartTimeColumns, inputEndTimeColumns, newFilterConditions, newGlobalCondition
        )
    }

    private fun createTemporalJoinInput(tableIndex: Int): TemporalJoinScannerInput {
        return TemporalJoinScannerInput(
            filterAndSortInputTable(tableIndex),
            inputTables[tableIndex].columns.size,
            startTimeColumnIndices[tableIndex],
            endTimeColumnIndices[tableIndex],
            inputStartTimeColumns[tableIndex].path == "_start_time"
        )
    }

    private fun filterAndSortInputTable(tableIndex: Int): RowScanner {
        var table = inputTables[tableIndex]
        // Try pushing down the filter operation if applicable
        val filter = filterConditionPerInput[tableIndex]
        var filterApplied = false
        val scratch = TypedValue()
        if (filter != null) {
            val filteredTable = table.filteredWith(filter)
            if (filteredTable != null) {
                table = filteredTable
                filterApplied = true
            }
        }
        // Identify the column to sort by (start time)
        val startTimeColumn = inputStartTimeColumns[tableIndex]
        val startTimeColumnLiteral = ColumnLiteral(startTimeColumn.path)
        // Try pushing down the sort operation
        val sortedTable = table.sortedBy(listOf(SortColumn(startTimeColumnLiteral, true)))
        if (sortedTable != null) {
            val scanner = sortedTable.scan()
            return if (filterApplied || filter == null) scanner else FilteringScanner(scanner) {
                ExpressionEvaluation.evaluate(filter, it, scratch).booleanValue
            }
        }
        // Otherwise, create a SortingScanner
        val startTimeColumnIndex = table.columns.indexOf(startTimeColumn)
        val baseScanner = if (filterApplied || filter == null) table.scan() else FilteringScanner(table.scan()) {
            ExpressionEvaluation.evaluate(filter, it, scratch).booleanValue
        }
        return SortingScanner(baseScanner, table.columns.size, listOf(IndexedSortColumn(startTimeColumnIndex, true)), emptyList())
    }

    companion object {

        fun from(inputTables: List<Table>): Table {
            require(inputTables.size >= 2) { "Cannot join less than two tables together" }

            // Ensure that none of the input tables have overlapping column names,
            // except for the special start_time and end_time columns
            val allColumnsByPath = mutableMapOf<String, Column>()
            for (inputTable in inputTables) {
                for (column in inputTable.columns) {
                    if (column.path == "_start_time" || column.path == "_end_time") continue
                    require(column.path !in allColumnsByPath) {
                        "Cannot join tables with duplicate column: ${column.path}"
                    }
                    allColumnsByPath[column.path] = column
                }
            }

            // Identify the time-related columns in each table
            val startTimeColumns = inputTables.map { findStartColumn(it.columns) }
            val endTimeColumns = inputTables.map { findEndColumn(it.columns) }

            return TemporalJoinTable(inputTables, startTimeColumns, endTimeColumns, inputTables.map { null }, null)
        }

        private fun findStartColumn(columns: List<Column>): Column {
            // Find column by reserved name
            val startTimeColumn = columns.find { it.name == "_start_time" }
            requireNotNull(startTimeColumn)
            return startTimeColumn
        }

        private fun findEndColumn(columns: List<Column>): Column {
            // Find column by reserved name
            val endTimeColumn = columns.find { it.name == "_end_time" }
            requireNotNull(endTimeColumn)
            return endTimeColumn
        }

    }

}