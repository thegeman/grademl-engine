package science.atlarge.grademl.query.execution.operators

import science.atlarge.grademl.query.execution.*
import science.atlarge.grademl.query.execution.IntTypes.toInt
import science.atlarge.grademl.query.language.FunctionDefinition
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.v2.*

class SortedAggregateOperator(
    private val input: QueryOperator,
    override val schema: TableSchema,
    groupByColumns: List<Int>,
    aggregateFunctions: List<FunctionDefinition>,
    private val aggregateFunctionTypes: List<Type>,
    private val aggregateFunctionArguments: List<List<PhysicalExpression>>,
    private val aggregateFunctionArgumentTypes: List<List<Type>>,
    private val aggregateColumns: List<Column>,
    private val projections: List<PhysicalExpression>
) : QueryOperator {

    private val groupByColumns = groupByColumns.toIntArray()
    private val groupByColumnTypes = groupByColumns.map { c -> input.schema.columns[c].type.toInt() }.toIntArray()

    private val aggregators = aggregateFunctions.mapIndexed { i, functionDefinition ->
        val implementation = BuiltinFunctionImplementations.from(functionDefinition)
        implementation as? AggregatingFunctionImplementation ?: throw IllegalStateException()
        implementation.newAggregator(aggregateFunctionArguments[i], aggregateFunctionArgumentTypes[i])
    }

    override fun execute(): TimeSeriesIterator {
        return SortedAggregateTimeSeriesIterator(
            input = input.execute(),
            schema = schema,
            startTimeColumn = input.schema.indexOfStartTimeColumn() ?: throw IllegalArgumentException(
                "Input to SortedAggregateOperator must have _start_time column"
            ),
            endTimeColumn = input.schema.indexOfEndTimeColumn() ?: throw IllegalArgumentException(
                "Input to SortedAggregateOperator must have _start_time column"
            ),
            groupByColumns = groupByColumns,
            groupByColumnTypes = groupByColumnTypes,
            aggregators = aggregators.toTypedArray(),
            aggregatorTypes = aggregateFunctionTypes.map { it.toInt() }.toIntArray(),
            aggregateColumns = aggregateColumns,
            projections = projections.toTypedArray()
        )
    }

}

private class SortedAggregateTimeSeriesIterator(
    private val input: TimeSeriesIterator,
    override val schema: TableSchema,
    private val startTimeColumn: Int,
    private val endTimeColumn: Int,
    private val groupByColumns: IntArray,
    private val groupByColumnTypes: IntArray,
    private val aggregators: Array<AggregatorV2>,
    private val aggregatorTypes: IntArray,
    private val aggregateColumns: List<Column>,
    private val projections: Array<PhysicalExpression>
) : TimeSeriesIterator {
    // Input types
    private val inputColumnTypes = input.schema.columns.map { it.type.toInt() }.toIntArray()

    // Compute and cache column counts
    private val inputColumnCount = input.schema.columns.size
    private val addedColumnCount = aggregators.size
    private val cacheColumnCount = inputColumnCount + addedColumnCount

    // Store values of first row in group and of completed aggregations for final projections
    private val rowBooleanValues = BooleanArray(cacheColumnCount)
    private val rowNumericValues = DoubleArray(cacheColumnCount)
    private val rowStringValues = arrayOfNulls<String?>(cacheColumnCount)

    private val intermediateRowWrapper = object : Row {
        override val schema = TableSchema(input.schema.columns + aggregateColumns)

        override fun getBoolean(columnIndex: Int) = rowBooleanValues[columnIndex]
        override fun getNumeric(columnIndex: Int) = rowNumericValues[columnIndex]
        override fun getString(columnIndex: Int) = rowStringValues[columnIndex]!!
    }

    // Track if we have peeked at the next time series
    private var inputTimeSeriesValid = false

    override val currentTimeSeries = object : TimeSeries {
        override val schema: TableSchema
            get() = this@SortedAggregateTimeSeriesIterator.schema

        override fun getBoolean(columnIndex: Int) =
            (projections[columnIndex] as BooleanPhysicalExpression).evaluateAsBoolean(intermediateRowWrapper)

        override fun getNumeric(columnIndex: Int) =
            (projections[columnIndex] as NumericPhysicalExpression).evaluateAsNumeric(intermediateRowWrapper)

        override fun getString(columnIndex: Int) =
            (projections[columnIndex] as StringPhysicalExpression).evaluateAsString(intermediateRowWrapper)

        override fun rowIterator() = object : RowIterator {
            private var isValid = true

            override val schema: TableSchema
                get() = this@SortedAggregateTimeSeriesIterator.schema
            override val currentRow = object : Row {
                override val schema: TableSchema
                    get() = this@SortedAggregateTimeSeriesIterator.schema

                override fun getBoolean(columnIndex: Int) =
                    (projections[columnIndex] as BooleanPhysicalExpression).evaluateAsBoolean(intermediateRowWrapper)

                override fun getNumeric(columnIndex: Int) =
                    (projections[columnIndex] as NumericPhysicalExpression).evaluateAsNumeric(intermediateRowWrapper)

                override fun getString(columnIndex: Int) =
                    (projections[columnIndex] as StringPhysicalExpression).evaluateAsString(intermediateRowWrapper)
            }

            override fun loadNext(): Boolean {
                if (!isValid) return false
                isValid = false
                return true
            }
        }
    }

    override fun loadNext(): Boolean {
        while (true) {
            // Process the next group
            if (!inputTimeSeriesValid && !input.loadNext()) return false
            inputTimeSeriesValid = true
            // Read the first row of the time series
            val ts = input.currentTimeSeries
            var inputRowIterator: RowIterator? = ts.rowIterator()
            if (!inputRowIterator!!.loadNext()) {
                inputTimeSeriesValid = false
                continue
            }
            val inputRow = inputRowIterator.currentRow
            // Cache its values
            for (c in 0 until inputColumnCount) {
                when (inputColumnTypes[c]) {
                    IntTypes.TYPE_BOOLEAN -> rowBooleanValues[c] = inputRow.getBoolean(c)
                    IntTypes.TYPE_NUMERIC -> rowNumericValues[c] = inputRow.getNumeric(c)
                    IntTypes.TYPE_STRING -> rowStringValues[c] = inputRow.getString(c)
                }
            }
            // Reset aggregators to prepare for aggregating this group of rows
            aggregators.forEach { it.reset() }
            // Aggregate every row in every time series matching the group-by column values just added to the cache
            do {
                // Add every row in the current time series to each aggregator
                aggregateRowsInTimeSeries(inputRowIterator)
                inputRowIterator = null
                // Read the next time series
                inputTimeSeriesValid = input.loadNext()
            } while (inputTimeSeriesValid && isTimeSeriesInGroup())
            // Finalize each aggregation
            for (aggregatorIndex in aggregators.indices) {
                val aggregator = aggregators[aggregatorIndex]
                val columnIndex = aggregatorIndex + inputColumnCount
                when (aggregatorTypes[aggregatorIndex]) {
                    IntTypes.TYPE_BOOLEAN -> rowBooleanValues[columnIndex] = aggregator.getBooleanResult()
                    IntTypes.TYPE_NUMERIC -> rowNumericValues[columnIndex] = aggregator.getNumericResult()
                    IntTypes.TYPE_STRING -> rowStringValues[columnIndex] = aggregator.getStringResult()
                }
            }
            return true
        }
    }

    private fun aggregateRowsInTimeSeries(peekedRowIterator: RowIterator?) {
        // Iterate over rows in time series
        val rows = peekedRowIterator ?: input.currentTimeSeries.rowIterator()
        var inputRowValid = peekedRowIterator != null
        var minStartTime = rowNumericValues[startTimeColumn]
        var maxEndTime = rowNumericValues[endTimeColumn]
        while (inputRowValid || rows.loadNext()) {
            val row = rows.currentRow
            inputRowValid = false
            // Add the next row to each aggregator
            for (a in aggregators) a.addRow(row)
            // Find the minimum start and maximum end time over all rows
            val startTime = row.getNumeric(startTimeColumn)
            val endTime = row.getNumeric(endTimeColumn)
            minStartTime = minOf(minStartTime, startTime)
            maxEndTime = maxOf(maxEndTime, endTime)
        }
        // Update the cached minimum start and maximum end time columns
        rowNumericValues[startTimeColumn] = minStartTime
        rowNumericValues[endTimeColumn] = maxEndTime
    }

    private fun isTimeSeriesInGroup(): Boolean {
        val timeSeries = input.currentTimeSeries
        // Compare against the cache on group-by columns
        groupByColumns.forEachIndexed { groupByIndex, columnIndex ->
            val isEqual = when (groupByColumnTypes[groupByIndex]) {
                IntTypes.TYPE_BOOLEAN -> rowBooleanValues[columnIndex] == timeSeries.getBoolean(columnIndex)
                IntTypes.TYPE_NUMERIC -> rowNumericValues[columnIndex] == timeSeries.getNumeric(columnIndex)
                IntTypes.TYPE_STRING -> rowStringValues[columnIndex] == timeSeries.getString(columnIndex)
                else -> throw IllegalArgumentException()
            }
            if (!isEqual) return false
        }
        return true
    }


}