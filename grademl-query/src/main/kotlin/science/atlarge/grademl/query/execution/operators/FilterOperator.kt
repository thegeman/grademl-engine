package science.atlarge.grademl.query.execution.operators

import science.atlarge.grademl.query.execution.BooleanPhysicalExpression
import science.atlarge.grademl.query.model.v2.*

class FilterOperator(
    private val input: QueryOperator,
    private val timeSeriesCondition: BooleanPhysicalExpression,
    private val rowCondition: BooleanPhysicalExpression
) : QueryOperator {

    override val schema: TableSchema
        get() = input.schema

    override fun execute(): TimeSeriesIterator =
        FilterTimeSeriesIterator(input.execute(), timeSeriesCondition, rowCondition)

}

private class FilterTimeSeriesIterator(
    private val input: TimeSeriesIterator,
    private val timeSeriesCondition: BooleanPhysicalExpression,
    private val rowCondition: BooleanPhysicalExpression
) : TimeSeriesIterator {

    private var peekedAtTimeSeries = false

    override val schema: TableSchema
        get() = input.schema
    override val currentTimeSeries
        get() = timeSeriesWrapper

    private val timeSeriesWrapper = object : TimeSeries {
        override val schema: TableSchema
            get() = input.schema

        override fun getBoolean(columnIndex: Int) = input.currentTimeSeries.getBoolean(columnIndex)
        override fun getNumeric(columnIndex: Int) = input.currentTimeSeries.getNumeric(columnIndex)
        override fun getString(columnIndex: Int) = input.currentTimeSeries.getString(columnIndex)

        private var cachedRowIterator: FilterRowIterator? = null

        override fun rowIterator(): RowIterator {
            if (cachedRowIterator == null) return input.currentTimeSeries.rowIterator()
            val result = cachedRowIterator!!
            cachedRowIterator = null
            return result
        }

        fun isNotEmpty(): Boolean {
            if (cachedRowIterator == null) {
                cachedRowIterator = FilterRowIterator(input.currentTimeSeries.rowIterator(), rowCondition)
            }
            return cachedRowIterator!!.isNotEmpty()
        }
    }

    override fun loadNext(): Boolean {
        // If a matching time series has already been loaded (by isNotEmpty), return it
        if (peekedAtTimeSeries) {
            peekedAtTimeSeries = false
            return true
        }
        // Find a time series matching the filter condition
        while (input.loadNext()) {
            if (timeSeriesCondition.evaluateAsBoolean(input.currentTimeSeries)) {
                // Check if the time series has any matching rows
                if (timeSeriesWrapper.isNotEmpty()) return true
            }
        }
        return false
    }

    fun isNotEmpty(): Boolean {
        if (peekedAtTimeSeries) return true
        if (!loadNext()) return false
        peekedAtTimeSeries = true
        return true
    }

}

private class FilterRowIterator(
    private val input: RowIterator,
    private val rowCondition: BooleanPhysicalExpression
) : RowIterator {

    private var peekedAtRow = false

    override val schema: TableSchema
        get() = input.schema
    override val currentRow: Row
        get() = input.currentRow

    override fun loadNext(): Boolean {
        // If a matching row has already been loaded (by isNotEmpty), return it
        if (peekedAtRow) {
            peekedAtRow = false
            return true
        }
        // Find a row matching the filter condition
        while (input.loadNext()) {
            if (rowCondition.evaluateAsBoolean(input.currentRow)) return true
        }
        return false
    }

    fun isNotEmpty(): Boolean {
        if (peekedAtRow) return true
        if (!loadNext()) return false
        peekedAtRow = true
        return true
    }

}