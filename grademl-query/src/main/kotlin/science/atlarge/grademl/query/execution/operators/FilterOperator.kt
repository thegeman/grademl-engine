package science.atlarge.grademl.query.execution.operators

import science.atlarge.grademl.query.execution.BooleanPhysicalExpression
import science.atlarge.grademl.query.model.v2.TableSchema
import science.atlarge.grademl.query.model.v2.TimeSeries
import science.atlarge.grademl.query.model.v2.TimeSeriesIterator

class FilterOperator(
    private val input: QueryOperator,
    private val condition: BooleanPhysicalExpression
) : QueryOperator {

    override val schema: TableSchema
        get() = input.schema

    override fun execute() = object : TimeSeriesIterator {
        private val inputIterator = input.execute()

        override val schema: TableSchema
            get() = this@FilterOperator.schema
        override lateinit var currentTimeSeries: TimeSeries
            private set

        override fun loadNext(): Boolean {
            while (inputIterator.loadNext()) {
                if (condition.evaluateAsBoolean(inputIterator.currentTimeSeries)) return true
            }
            return false
        }
    }

}