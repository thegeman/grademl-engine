package science.atlarge.grademl.query.execution.operators

import science.atlarge.grademl.query.execution.*
import science.atlarge.grademl.query.model.RowIterator
import science.atlarge.grademl.query.model.TableSchema
import science.atlarge.grademl.query.model.TimeSeriesIterator

class ProjectOperator(
    private val input: QueryOperator,
    override val schema: TableSchema,
    private val columnExpressions: List<PhysicalExpression>
) : QueryOperator {

    override fun execute(): TimeSeriesIterator =
        ProjectTimeSeriesIterator(input.execute(), columnExpressions)

}

private class ProjectTimeSeriesIterator(
    private val input: TimeSeriesIterator,
    columnExpressions: List<PhysicalExpression>
) : AbstractTimeSeriesIterator<ProjectRowIterator>(input.schema) {

    private val booleanColumnExpressions = Array(columnExpressions.size) { i ->
        columnExpressions[i] as? BooleanPhysicalExpression
    }
    private val numericColumnExpressions = Array(columnExpressions.size) { i ->
        columnExpressions[i] as? NumericPhysicalExpression
    }
    private val stringColumnExpressions = Array(columnExpressions.size) { i ->
        columnExpressions[i] as? StringPhysicalExpression
    }

    override fun getBoolean(columnIndex: Int) =
        booleanColumnExpressions[columnIndex]!!.evaluateAsBoolean(input.currentTimeSeries)

    override fun getNumeric(columnIndex: Int) =
        numericColumnExpressions[columnIndex]!!.evaluateAsNumeric(input.currentTimeSeries)

    override fun getString(columnIndex: Int) =
        stringColumnExpressions[columnIndex]!!.evaluateAsString(input.currentTimeSeries)

    override fun createRowIterator() =
        ProjectRowIterator(schema, booleanColumnExpressions, numericColumnExpressions, stringColumnExpressions)

    override fun resetRowIteratorWithCurrentTimeSeries(rowIterator: ProjectRowIterator) {
        rowIterator.input = input.currentTimeSeries.rowIterator()
    }

    override fun internalLoadNext() = input.loadNext()

}

private class ProjectRowIterator(
    schema: TableSchema,
    private val booleanColumnExpressions: Array<BooleanPhysicalExpression?>,
    private val numericColumnExpressions: Array<NumericPhysicalExpression?>,
    private val stringColumnExpressions: Array<StringPhysicalExpression?>
) : AbstractRowIterator(schema) {

    lateinit var input: RowIterator

    override fun getBoolean(columnIndex: Int) =
        booleanColumnExpressions[columnIndex]!!.evaluateAsBoolean(input.currentRow)

    override fun getNumeric(columnIndex: Int) =
        numericColumnExpressions[columnIndex]!!.evaluateAsNumeric(input.currentRow)

    override fun getString(columnIndex: Int) =
        stringColumnExpressions[columnIndex]!!.evaluateAsString(input.currentRow)

    override fun loadNext() = input.loadNext()

}
