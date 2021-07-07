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
        inputTable.columns.filter { c -> c.path != "start_time" && c.path != "end_time" }
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
        Column("start_time", "start_time", Type.NUMERIC, ColumnFunction.TIME_START),
        Column("end_time", "end_time", Type.NUMERIC, ColumnFunction.TIME_END)
    ) + columnsPerInput.flatten()

    override fun scan(): RowScanner {
        // Create an input for a temporal join scanner for every input table
        val joinInputs = inputTables.indices.map { createTemporalJoinInput(it) }
        // Construct a nested scanner from binary temporal joins
        var compoundScanner = TemporalJoinScanner(joinInputs[0], joinInputs[1])
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
            inputStartTimeColumns[tableIndex].path == "start_time"
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
        val sortedTable = table.sortedBy(listOf(startTimeColumnLiteral))
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
        return SortingScanner(baseScanner, table.columns.size, listOf(startTimeColumnIndex), emptyList())
    }

    companion object {

        fun from(inputTables: List<Table>): Table {
            require(inputTables.size >= 2) { "Cannot join less than two tables together" }

            // Ensure that none of the input tables have overlapping column names,
            // except for the special start_time and end_time columns
            val allColumnsByPath = mutableMapOf<String, Column>()
            for (inputTable in inputTables) {
                for (column in inputTable.columns) {
                    if (column.path == "start_time" || column.path == "end_time") continue
                    require(column.path !in allColumnsByPath) {
                        "Cannot join tables with duplicate column: ${column.path}"
                    }
                    allColumnsByPath[column.path] = column
                }
            }

            // Identify the time-related columns in each table
            val startTimeColumns = inputTables.map { findStartColumn(it.columns) }
            val endTimeColumns = inputTables.map { findEndColumn(it.columns) }

            // Sanity check the selected time columns; start and end time should either
            // both be given by reserved columns or both be given by columns with explicit function
            for (i in startTimeColumns.indices) {
                require((startTimeColumns[i].path == "start_time") == (endTimeColumns[i].path == "end_time"))
            }

            return TemporalJoinTable(inputTables, startTimeColumns, endTimeColumns, inputTables.map { null }, null)
        }

        private fun findStartColumn(columns: List<Column>): Column {
            // Find column by reserved name
            val startTimeColumn = columns.find { it.path == "start_time" }
            if (startTimeColumn != null) return startTimeColumn
            // Otherwise, find all columns with the start time function
            val columnsWithFunction = columns.filter { it.function == ColumnFunction.TIME_START }
            require(columnsWithFunction.size == 1) {
                "Inputs of a TEMPORAL JOIN must have precisely one column indicating the start time of an event"
            }
            return columnsWithFunction[0]
        }

        private fun findEndColumn(columns: List<Column>): Column {
            // Find column by reserved name
            val endTimeColumn = columns.find { it.path == "end_time" }
            if (endTimeColumn != null) return endTimeColumn
            // Otherwise, find all columns with the end time function
            val columnsWithFunction = columns.filter { it.function == ColumnFunction.TIME_END }
            require(columnsWithFunction.size == 1) {
                "Inputs of a TEMPORAL JOIN must have precisely one column indicating the end time of an event"
            }
            return columnsWithFunction[0]
        }

    }

}