package science.atlarge.grademl.query.execution.impl

import science.atlarge.grademl.query.analysis.AggregateFunctionDecomposition
import science.atlarge.grademl.query.execution.AggregatingFunctionImplementation
import science.atlarge.grademl.query.execution.Aggregator
import science.atlarge.grademl.query.execution.BuiltinFunctionImplementations
import science.atlarge.grademl.query.execution.ExpressionEvaluation
import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.*
import science.atlarge.grademl.query.nextOrNull

class ProjectingScanner(
    private val baseScanner: (() -> RowScanner)? = null,
    private val baseGroupScanner: (() -> RowGroupScanner)? = null,
    columnExpressions: List<Expression>,
    private val inputColumns: List<Column>
) : RowScanner() {

    private val inputIsGrouped = baseGroupScanner != null

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

    private var linearInputScanner: RowScanner? = null

    private val intermediateRowWrapper: IntermediateRowWrapper
    private val perGroupOutputRowWrapper: PerGroupOutputRowWrapper
    private val linearOutputRowWrapper: LinearOutputRowWrapper

    init {
        require((baseScanner == null) != (baseGroupScanner == null)) {
            "Precisely one of baseScanner and baseGroupScanner must be provided"
        }

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

        this.rowGroupScanners = (0..this.maxFunctionDepth).map {
            baseGroupScanner?.invoke() ?: object : RowGroupScanner() {
                    private var depleted = false
                    override fun fetchRowGroup(): RowGroup? {
                        if (depleted) return null
                        depleted = true
                        return baseScanner!!()
                    }
                }
        }

        this.firstRowValues = Array(inputColumns.size) { TypedValue() }

        this.intermediateRowWrapper = IntermediateRowWrapper()
        this.perGroupOutputRowWrapper = PerGroupOutputRowWrapper()
        this.linearOutputRowWrapper = LinearOutputRowWrapper()
    }

    override fun fetchRow(): Row? {
        // For grouped inputs, produce one row per group
        if (inputIsGrouped) {
            // Compute all aggregate functions for the next group of rows
            if (!aggregateNextRowGroup()) return null
            // Return the projected output row
            return perGroupOutputRowWrapper
        }
        // For non-grouped inputs, compute aggregate functions once and produce one row per input row
        if (linearInputScanner == null) {
            // Compute all aggregate functions for the entire input table
            if (!aggregateNextRowGroup()) return null
            linearInputScanner = baseScanner!!.invoke()
        }
        // Return the projected output row
        val nextInputRow = linearInputScanner!!.nextOrNull() ?: return null
        linearOutputRowWrapper.setInputRow(nextInputRow)
        return linearOutputRowWrapper
    }

    private fun aggregateNextRowGroup(): Boolean {
        for (depth in maxFunctionDepth downTo 0) {
            // Start a new row group for the set of aggregating functions at a given depth
            val rowGroup = rowGroupScanners[depth].nextOrNull() ?: return false
            val functionsAtDepth = aggregateFunctionsByDepth[depth] ?: emptyList()
            // Compute each function
            computeFunctions(functionsAtDepth, rowGroup, saveFirstRow = depth == 0 && inputIsGrouped)
        }
        return true
    }

    private fun computeFunctions(functionIds: List<Int>, scanner: RowScanner, saveFirstRow: Boolean) {
        // Initialize the aggregations
        for (f in functionIds) {
            functionAggregators[f].reset()
        }
        // Add every row in the group to each function aggregator
        var isFirstRow = saveFirstRow
        for (row in scanner) {
            // Store the values of the first row, if needed
            if (isFirstRow) {
                for (i in firstRowValues.indices) row.readValue(i, firstRowValues[i])
                isFirstRow = false
            }
            // For every aggregating function, compute its arguments and add them in
            intermediateRowWrapper.baseRow = row
            for (f in functionIds) {
                val argumentArray = functionArgumentValues[f]
                for (argId in argumentArray.indices) {
                    ExpressionEvaluation.evaluate(
                        aggregateFunctionArguments[f][argId],
                        intermediateRowWrapper,
                        argumentArray[argId]
                    )
                }
                functionAggregators[f].addRow(argumentArray)
            }
        }
        // Finalize the aggregations
        for (f in functionIds) {
            functionAggregators[f].writeResultTo(functionOutputs[f])
        }
    }

    private inner class IntermediateRowWrapper : Row {

        private val baseRowWidth = inputColumns.size
        lateinit var baseRow: Row

        override val columnCount = baseRowWidth + functionOutputs.size

        override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
            if (columnId < baseRowWidth) baseRow.readValue(columnId, outValue)
            else functionOutputs[columnId - baseRowWidth].copyTo(outValue)
            return outValue
        }

    }

    private inner class PerGroupOutputRowWrapper : Row {

        private val inputRow = object : Row {

            private val baseRowWidth = firstRowValues.size

            override val columnCount = baseRowWidth + functionOutputs.size

            override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
                if (columnId < baseRowWidth) firstRowValues[columnId].copyTo(outValue)
                else functionOutputs[columnId - baseRowWidth].copyTo(outValue)
                return outValue
            }

        }

        override val columnCount = columnExpressions.size

        override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
            return ExpressionEvaluation.evaluate(columnExpressions[columnId], inputRow, outValue)
        }

    }

    private inner class LinearOutputRowWrapper : Row {

        private val inputRow = IntermediateRowWrapper()
        fun setInputRow(row: Row) {
            inputRow.baseRow = row
        }

        override val columnCount = columnExpressions.size

        override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
            return ExpressionEvaluation.evaluate(columnExpressions[columnId], inputRow, outValue)
        }

    }

}