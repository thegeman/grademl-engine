package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.analysis.AggregateFunctionDecomposition
import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.*
import science.atlarge.grademl.query.nextOrNull

class ProjectedTable(
    val baseTable: Table,
    val columnExpressions: List<Expression>,
    columnNames: List<String>
) : Table {

    override val columns: List<Column>

    override val isGrouped: Boolean
        get() = false
    override val supportsPushDownFilters: Boolean
        get() = false
    override val supportsPushDownProjections: Boolean
        get() = false
    override val supportsPushDownSort: Boolean
        get() = false
    override val supportsPushDownGroupBy: Boolean
        get() = false

    init {
        require(columnExpressions.size == columnNames.size)
        columns = columnNames.mapIndexed { index, columnName ->
            Column(columnName, columnName, columnExpressions[index].type)
        }
    }

    override fun scan(): RowScanner {
        return ProjectedTableScanner(baseTable, columnExpressions, baseTable.columns)
    }

}

private class ProjectedTableScanner(
    private val baseTable: Table,
    columnExpressions: List<Expression>,
    private val inputColumns: List<Column>
) : RowScanner() {

    private val hasClusteredInput = baseTable.isGrouped
    private var nonClusteredScanner: RowScanner? = null

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

    private val firstRowWrapper: Row
    private val inputRowProxy: Row.Proxy
    private val computedRowWrapper: Row
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
                baseTable.scanGroups() as RowGroupScanner
            } else {
                object : RowGroupScanner() {
                    private var depleted = false
                    override fun fetchRowGroup(): RowGroup? {
                        if (depleted) return null
                        depleted = true
                        return baseTable.scan()
                    }
                }
            }
        }
        this.firstRowValues = Array(inputColumns.size) { TypedValue() }

        this.firstRowWrapper = Row.wrap(this.firstRowValues)
        this.inputRowProxy = Row.Proxy()
        this.computedRowWrapper = Row.concat(this.inputRowProxy, Row.wrap(this.functionOutputs))
        this.outputRowWrapper = ProjectedTableRow(this.columnExpressions, this.computedRowWrapper)
    }

    override fun fetchRow(): Row? {
        // For clustered inputs, produce one row per group
        if (hasClusteredInput) {
            if (!processNextRowGroup()) return null
            inputRowProxy.baseRow = firstRowWrapper
            return outputRowWrapper
        }
        // For non-clustered inputs, compute aggregate functions once and produce one row per input row
        if (nonClusteredScanner == null) {
            if (!processNextRowGroup()) return null
            nonClusteredScanner = baseTable.scan()
        }
        inputRowProxy.baseRow = nonClusteredScanner!!.nextOrNull() ?: return null
        return outputRowWrapper
    }

    private fun processNextRowGroup(): Boolean {
        for (depth in maxFunctionDepth downTo 0) {
            // Start a new row group for the set of aggregating functions at a given depth
            val rowGroup = rowGroupScanners[depth].nextOrNull() ?: return false
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
                        row.readValue(columnIndex, firstRowValues[columnIndex])
                    }
                }
                isFirstRow = false
                // For every aggregating function, compute its arguments and add them in
                inputRowProxy.baseRow = row
                for (f in functionsAtDepth) {
                    val argumentArray = functionArgumentValues[f]
                    for (argId in argumentArray.indices) {
                        val argumentExpression = aggregateFunctionArguments[f][argId]
                        if (argumentExpression.type == Type.UNDEFINED) throw IllegalArgumentException()
                        else ExpressionEvaluation.evaluate(argumentExpression, computedRowWrapper, argumentArray[argId])
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

private class ProjectedTableRow(
    private val columnExpressions: List<Expression>,
    private val inputRow: Row
) : Row {

    override val columnCount = columnExpressions.size

    override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
        if (columnExpressions[columnId].type == Type.UNDEFINED) outValue.clear()
        else ExpressionEvaluation.evaluate(columnExpressions[columnId], inputRow, outValue)
        return outValue
    }

}