package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.analysis.AggregateFunctionDecomposition
import science.atlarge.grademl.query.ensureExhaustive
import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.*

class ProjectedTable(
    val baseTable: Table,
    val columnExpressions: List<Expression>,
    columnNames: List<String>
) : Table {

    override val columns: List<Column>

    init {
        require(columnExpressions.size == columnNames.size)
        columns = columnNames.mapIndexed { index, columnName ->
            Column(columnName, columnName, columnExpressions[index].type)
        }
    }

    override fun scan(): RowScanner {
        return ProjectedTableScanner({ baseTable.scan() }, columnExpressions, baseTable.columns)
    }

}

private class ProjectedTableScanner(
    scannerProvider: () -> RowScanner,
    columnExpressions: List<Expression>,
    private val inputColumns: List<Column>
) : RowScanner {

    private val topLevelScanner = scannerProvider()
    private val hasClusteredInput = topLevelScanner is RowGroupScanner

    private val columnExpressions: List<Expression>
    private val aggregateFunctions: List<AggregatingFunctionImplementation>
    private val aggregateFunctionDepths: List<Int>
    private val aggregateFunctionTypes: List<Type>
    private val aggregateFunctionArguments: List<List<Expression>>

    private val aggregateFunctionsByDepth: Map<Int, List<Int>>
    private val maxFunctionDepth: Int

    private val functionArgumentValues: List<Array<TypedValue>>
    private val functionAggregators: Array<Aggregator>
    private val functionOutputs: Array<TypedValue>

    private val rowGroupScanners: List<RowGroupScanner>
    private val firstRowValues: Array<TypedValue>
    private var firstGroupProcessed = false

    private val inputRowWrapper: ConcatRow
    private val computedRowWrapper: ConcatRow
    private val outputRowWrapper: ProjectedTableRow

    init {
        val aggregateFunctionDecomposition =
            AggregateFunctionDecomposition.decompose(columnExpressions, inputColumns.size)
        this.columnExpressions = aggregateFunctionDecomposition.rewrittenExpressions
        this.aggregateFunctions = aggregateFunctionDecomposition.aggregateFunctions.map { f ->
            BuiltinFunctionImplementations.from(f) as AggregatingFunctionImplementation
        }
        this.aggregateFunctionDepths = aggregateFunctionDecomposition.aggregateFunctionDepths
        this.aggregateFunctionTypes = aggregateFunctionDecomposition.aggregateFunctionTypes
        this.aggregateFunctionArguments = aggregateFunctionDecomposition.rewrittenFunctionArguments

        this.aggregateFunctionsByDepth = this.aggregateFunctionDepths.mapIndexed { index, depth -> depth to index }
            .groupBy({ it.first }, { it.second })
        this.maxFunctionDepth = this.aggregateFunctionDepths.maxOrNull() ?: 0

        this.functionArgumentValues = this.aggregateFunctionArguments.map { Array(it.size) { TypedValue() } }
        this.functionAggregators = Array(this.aggregateFunctions.size) { i ->
            this.aggregateFunctions[i].newAggregator(this.aggregateFunctionArguments[i].map { it.type })
        }
        this.functionOutputs = Array(this.aggregateFunctions.size) { TypedValue() }

        this.rowGroupScanners = (0..this.maxFunctionDepth).map { i ->
            if (hasClusteredInput) {
                (if (i == 0) topLevelScanner else scannerProvider()) as RowGroupScanner
            } else {
                object : RowGroupScanner {
                    private var depleted = false
                    override fun nextRowGroup(): RowGroup? {
                        if (depleted) return null
                        depleted = true
                        return scannerProvider()
                    }
                }
            }
        }
        this.firstRowValues = Array(inputColumns.size) { TypedValue() }

        this.inputRowWrapper = ConcatRow(0, this.firstRowValues)
        this.computedRowWrapper = ConcatRow(inputColumns.size, this.functionOutputs)
        this.outputRowWrapper = ProjectedTableRow(this.columnExpressions, this.computedRowWrapper)
    }

    override fun nextRow(): Row? {
        // For clustered inputs, produce one row per group
        if (hasClusteredInput) {
            if (!processNextRowGroup()) return null
            computedRowWrapper.baseRow = inputRowWrapper
            return outputRowWrapper
        }
        // For non-clustered inputs, compute aggregate functions once and produce one row per input row
        if (!firstGroupProcessed) {
            if (!processNextRowGroup()) return null
            firstGroupProcessed = true
        }
        computedRowWrapper.baseRow = topLevelScanner.nextRow() ?: return null
        return outputRowWrapper
    }

    private fun processNextRowGroup(): Boolean {
        for (depth in maxFunctionDepth downTo 0) {
            // Start a new row group for the set of aggregating functions at a given depth
            val rowGroup = rowGroupScanners[depth].nextRowGroup() ?: return false
            val functionsAtDepth = aggregateFunctionsByDepth[depth] ?: emptyList()
            // Initialize the aggregations at this level
            for (f in functionsAtDepth) {
                functionAggregators[f].reset()
            }
            // Add every row in the group to each function aggregator
            var isFirstRow = true
            for (row in rowGroup) {
                // Cache the first row in the group for the top-level computation
                if (isFirstRow && hasClusteredInput && depth == 0) {
                    inputColumns.forEachIndexed { columnIndex, column ->
                        when (column.type) {
                            Type.UNDEFINED -> {}
                            Type.BOOLEAN -> firstRowValues[columnIndex].booleanValue = row.readBoolean(columnIndex)
                            Type.NUMERIC -> firstRowValues[columnIndex].numericValue = row.readNumeric(columnIndex)
                            Type.STRING -> firstRowValues[columnIndex].stringValue = row.readString(columnIndex)
                        }.ensureExhaustive
                    }
                }
                isFirstRow = false
                // For every aggregating function, compute its arguments and add them in
                computedRowWrapper.baseRow = row
                for (f in functionsAtDepth) {
                    val argumentArray = functionArgumentValues[f]
                    for (argId in argumentArray.indices) {
                        val argumentExpression = aggregateFunctionArguments[f][argId]
                        when (argumentExpression.type) {
                            Type.UNDEFINED -> throw IllegalArgumentException()
                            Type.BOOLEAN -> argumentArray[argId].booleanValue =
                                ExpressionEvaluation.evaluateAsBoolean(argumentExpression, computedRowWrapper)
                            Type.NUMERIC -> argumentArray[argId].numericValue =
                                ExpressionEvaluation.evaluateAsNumeric(argumentExpression, computedRowWrapper)
                            Type.STRING -> argumentArray[argId].stringValue =
                                ExpressionEvaluation.evaluateAsString(argumentExpression, computedRowWrapper)
                        }.ensureExhaustive
                    }
                    functionAggregators[f].addRow(argumentArray)
                }
            }
            // Finalize the aggregations at this level
            for (f in functionsAtDepth) {
                functionAggregators[f].writeResultTo(functionOutputs[f])
            }
        }
        return true
    }

}

private class ConcatRow(
    private val baseRowWidth: Int,
    private val addedColumns: Array<TypedValue>
) : Row {
    lateinit var baseRow: Row
    override fun readBoolean(columnId: Int) = when {
        columnId >= baseRowWidth -> addedColumns[columnId - baseRowWidth].booleanValue
        else -> baseRow.readBoolean(columnId)
    }

    override fun readNumeric(columnId: Int) = when {
        columnId >= baseRowWidth -> addedColumns[columnId - baseRowWidth].numericValue
        else -> baseRow.readNumeric(columnId)
    }

    override fun readString(columnId: Int) = when {
        columnId >= baseRowWidth -> addedColumns[columnId - baseRowWidth].stringValue
        else -> baseRow.readString(columnId)
    }
}

private class ProjectedTableRow(
    private val columnExpressions: List<Expression>,
    private val inputRow: Row
) : Row {

    override fun readBoolean(columnId: Int): Boolean {
        return ExpressionEvaluation.evaluateAsBoolean(columnExpressions[columnId], inputRow)
    }

    override fun readNumeric(columnId: Int): Double {
        return ExpressionEvaluation.evaluateAsNumeric(columnExpressions[columnId], inputRow)
    }

    override fun readString(columnId: Int): String {
        return ExpressionEvaluation.evaluateAsString(columnExpressions[columnId], inputRow)
    }

}