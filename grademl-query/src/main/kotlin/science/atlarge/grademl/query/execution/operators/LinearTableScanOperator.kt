package science.atlarge.grademl.query.execution.operators

import science.atlarge.grademl.query.model.Table
import science.atlarge.grademl.query.model.TableSchema
import science.atlarge.grademl.query.model.TimeSeriesIterator

class LinearTableScanOperator(
    private val table: Table
) : QueryOperator {

    override val schema: TableSchema
        get() = table.schema

    override fun execute(): TimeSeriesIterator {
        return table.timeSeriesIterator()
    }

}