package science.atlarge.grademl.query.execution.operators

import science.atlarge.grademl.query.model.v2.Table
import science.atlarge.grademl.query.model.v2.TableSchema
import science.atlarge.grademl.query.model.v2.TimeSeriesIterator

class LinearTableScanOperator(
    private val table: Table
) : QueryOperator {

    override val schema: TableSchema
        get() = table.schema

    override fun execute(): TimeSeriesIterator {
        return table.timeSeriesIterator()
    }

}