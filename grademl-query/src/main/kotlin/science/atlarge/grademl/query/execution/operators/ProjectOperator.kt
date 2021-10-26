package science.atlarge.grademl.query.execution.operators

import science.atlarge.grademl.query.execution.BooleanPhysicalExpression
import science.atlarge.grademl.query.execution.NumericPhysicalExpression
import science.atlarge.grademl.query.execution.PhysicalExpression
import science.atlarge.grademl.query.execution.StringPhysicalExpression
import science.atlarge.grademl.query.model.*

class ProjectOperator(
    private val input: QueryOperator,
    override val schema: TableSchema,
    columnExpressions: List<PhysicalExpression>
) : QueryOperator {

    private val booleanColumnExpressions = Array(columnExpressions.size) { i ->
        columnExpressions[i] as? BooleanPhysicalExpression
    }
    private val numericColumnExpressions = Array(columnExpressions.size) { i ->
        columnExpressions[i] as? NumericPhysicalExpression
    }
    private val stringColumnExpressions = Array(columnExpressions.size) { i ->
        columnExpressions[i] as? StringPhysicalExpression
    }

    override fun execute(): TimeSeriesIterator = ProjectTimeSeriesIterator(
        input.execute(), schema, booleanColumnExpressions, numericColumnExpressions, stringColumnExpressions
    )

}

private class ProjectTimeSeriesIterator(
    private val inputIterator: TimeSeriesIterator,
    override val schema: TableSchema,
    private val booleanColumnExpressions: Array<BooleanPhysicalExpression?>,
    private val numericColumnExpressions: Array<NumericPhysicalExpression?>,
    private val stringColumnExpressions: Array<StringPhysicalExpression?>
) : TimeSeriesIterator {

    private val projectTimeSeries = object : TimeSeries {
        lateinit var input: TimeSeries

        override val schema: TableSchema
            get() = this@ProjectTimeSeriesIterator.schema

        override fun getBoolean(columnIndex: Int) = booleanColumnExpressions[columnIndex]!!.evaluateAsBoolean(input)
        override fun getNumeric(columnIndex: Int) = numericColumnExpressions[columnIndex]!!.evaluateAsNumeric(input)
        override fun getString(columnIndex: Int) = stringColumnExpressions[columnIndex]!!.evaluateAsString(input)

        override fun rowIterator(): RowIterator = ProjectRowIterator(
            input.rowIterator(), schema, booleanColumnExpressions, numericColumnExpressions, stringColumnExpressions
        )
    }
    override val currentTimeSeries: TimeSeries
        get() = projectTimeSeries

    override fun loadNext(): Boolean {
        if (!inputIterator.loadNext()) return false
        projectTimeSeries.input = inputIterator.currentTimeSeries
        return true
    }

}

class ProjectRowIterator(
    private val inputIterator: RowIterator,
    override val schema: TableSchema,
    private val booleanColumnExpressions: Array<BooleanPhysicalExpression?>,
    private val numericColumnExpressions: Array<NumericPhysicalExpression?>,
    private val stringColumnExpressions: Array<StringPhysicalExpression?>
) : RowIterator {

    private val projectRow = object : Row {
        lateinit var input: Row

        override val schema: TableSchema
            get() = this@ProjectRowIterator.schema

        override fun getBoolean(columnIndex: Int) = booleanColumnExpressions[columnIndex]!!.evaluateAsBoolean(input)
        override fun getNumeric(columnIndex: Int) = numericColumnExpressions[columnIndex]!!.evaluateAsNumeric(input)
        override fun getString(columnIndex: Int) = stringColumnExpressions[columnIndex]!!.evaluateAsString(input)
    }
    override val currentRow: Row
        get() = projectRow

    override fun loadNext(): Boolean {
        if (!inputIterator.loadNext()) return false
        projectRow.input = inputIterator.currentRow
        return true
    }

}
