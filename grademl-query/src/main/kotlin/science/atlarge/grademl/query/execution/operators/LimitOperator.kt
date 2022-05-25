package science.atlarge.grademl.query.execution.operators

import science.atlarge.grademl.query.execution.AccountingRowIterator
import science.atlarge.grademl.query.execution.AccountingTimeSeriesIterator
import science.atlarge.grademl.query.model.RowIterator
import science.atlarge.grademl.query.model.TableSchema
import science.atlarge.grademl.query.model.TimeSeriesIterator

class LimitOperator(
    private val input: QueryOperator,
    private val limit: Int
) : AccountingQueryOperator() {

    override val schema: TableSchema
        get() = input.schema

    override fun createTimeSeriesIterator(): AccountingTimeSeriesIterator<*> =
        LimitTimeSeriesIterator(input.execute(), limit)

}

private class LimitTimeSeriesIterator(
    private val input: TimeSeriesIterator,
    limit: Int
) : AccountingTimeSeriesIterator<LimitRowIterator>(input.schema) {

    private var remaining = if (limit < 0) -1 else limit
    private var nextRemaining = remaining

    override fun getBoolean(columnIndex: Int) = input.currentTimeSeries.getBoolean(columnIndex)
    override fun getNumeric(columnIndex: Int) = input.currentTimeSeries.getNumeric(columnIndex)
    override fun getString(columnIndex: Int) = input.currentTimeSeries.getString(columnIndex)

    override fun createRowIterator() = LimitRowIterator(input.schema) {
        nextRemaining = minOf(nextRemaining, it)
    }

    override fun resetRowIteratorWithCurrentTimeSeries(rowIterator: LimitRowIterator) {
        rowIterator.input = input.currentTimeSeries.rowIterator()
        rowIterator.remaining = remaining
    }

    override fun internalLoadNext(): Boolean {
        remaining = nextRemaining
        if (remaining == 0) return false
        if (!input.loadNext()) return false
        return true
    }

}

private class LimitRowIterator(
    schema: TableSchema,
    private val reportRemaining: (Int) -> Unit
) : AccountingRowIterator(schema) {

    var remaining = 0
    lateinit var input: RowIterator

    override fun getBoolean(columnIndex: Int) = input.currentRow.getBoolean(columnIndex)
    override fun getNumeric(columnIndex: Int) = input.currentRow.getNumeric(columnIndex)
    override fun getString(columnIndex: Int) = input.currentRow.getString(columnIndex)

    override fun internalLoadNext(): Boolean {
        if (remaining == 0) return false
        if (!input.loadNext()) return false
        if (remaining > 0) {
            remaining--
            reportRemaining(remaining)
        }
        return true
    }

}