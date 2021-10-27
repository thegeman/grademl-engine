package science.atlarge.grademl.query.execution.operators

import science.atlarge.grademl.query.execution.AbstractRowIterator
import science.atlarge.grademl.query.execution.AbstractTimeSeriesIterator
import science.atlarge.grademl.query.execution.BooleanPhysicalExpression
import science.atlarge.grademl.query.model.RowIterator
import science.atlarge.grademl.query.model.TableSchema
import science.atlarge.grademl.query.model.TimeSeriesIterator

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
) : AbstractTimeSeriesIterator<FilterRowIterator>(input.schema) {

    private var peekedInputRowIterator: RowIterator? = null

    override fun getBoolean(columnIndex: Int) = input.currentTimeSeries.getBoolean(columnIndex)
    override fun getNumeric(columnIndex: Int) = input.currentTimeSeries.getNumeric(columnIndex)
    override fun getString(columnIndex: Int) = input.currentTimeSeries.getString(columnIndex)

    override fun createRowIterator() = FilterRowIterator(schema, rowCondition)

    override fun resetRowIteratorWithCurrentTimeSeries(rowIterator: FilterRowIterator) {
        if (peekedInputRowIterator != null) {
            rowIterator.input = peekedInputRowIterator!!
            rowIterator.peekedAtInput = true
            peekedInputRowIterator = null
        } else {
            rowIterator.input = input.currentTimeSeries.rowIterator()
            rowIterator.peekedAtInput = false
        }
    }

    override fun internalLoadNext(): Boolean {
        // Find a time series matching the filter condition
        while (input.loadNext()) {
            if (timeSeriesCondition.evaluateAsBoolean(input.currentTimeSeries)) {
                // Check if the time series has any matching rows
                val rowIterator = input.currentTimeSeries.rowIterator()
                while (rowIterator.loadNext()) {
                    // If any row matches the filter condition, return this time series
                    if (rowCondition.evaluateAsBoolean(rowIterator.currentRow)) {
                        peekedInputRowIterator = rowIterator
                        return true
                    }
                }
            }
        }
        return false
    }

}

private class FilterRowIterator(
    schema: TableSchema,
    private val rowCondition: BooleanPhysicalExpression
) : AbstractRowIterator(schema) {

    lateinit var input: RowIterator
    var peekedAtInput = false

    override fun getBoolean(columnIndex: Int) = input.currentRow.getBoolean(columnIndex)
    override fun getNumeric(columnIndex: Int) = input.currentRow.getNumeric(columnIndex)
    override fun getString(columnIndex: Int) = input.currentRow.getString(columnIndex)

    override fun loadNext(): Boolean {
        // If a matching row has already been loaded (to check if any matching rows exist), return it
        if (peekedAtInput) {
            peekedAtInput = false
            return true
        }
        // Find a row matching the filter condition
        while (input.loadNext()) {
            if (rowCondition.evaluateAsBoolean(input.currentRow)) return true
        }
        return false
    }

}