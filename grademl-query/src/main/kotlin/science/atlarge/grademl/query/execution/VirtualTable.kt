package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.model.Table
import science.atlarge.grademl.query.model.TableSchema
import science.atlarge.grademl.query.model.TimeSeriesIterator
import science.atlarge.grademl.query.plan.physical.PhysicalQueryPlan

class VirtualTable(
    val queryPlan: PhysicalQueryPlan
) : Table {

    override val schema: TableSchema
        get() = queryPlan.schema

    override fun timeSeriesIterator(): TimeSeriesIterator {
        return queryPlan.toQueryOperator().execute()
    }

}