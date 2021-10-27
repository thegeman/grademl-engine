package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.model.RowIterator
import science.atlarge.grademl.query.model.TableSchema
import science.atlarge.grademl.query.model.TimeSeries
import science.atlarge.grademl.query.model.TimeSeriesIterator

abstract class AbstractTimeSeriesIterator<T : AbstractRowIterator>(
    final override val schema: TableSchema
) : TimeSeriesIterator, TimeSeries {

    final override val currentTimeSeries: TimeSeries
        get() = this

    private val rowIterators = mutableListOf<T>()
    private var rowIteratorsInUse = 0

    protected abstract fun createRowIterator(): T
    protected abstract fun resetRowIteratorWithCurrentTimeSeries(rowIterator: T)

    final override fun rowIterator(): RowIterator {
        if (rowIteratorsInUse == rowIterators.size) rowIterators.add(createRowIterator())
        val rowIterator = rowIterators[rowIteratorsInUse++]
        resetRowIteratorWithCurrentTimeSeries(rowIterator)
        return rowIterator
    }

    protected abstract fun internalLoadNext(): Boolean

    final override fun loadNext(): Boolean {
        // Reset the iterator count
        rowIteratorsInUse = 0
        // Load the next time series
        return internalLoadNext()
    }

}