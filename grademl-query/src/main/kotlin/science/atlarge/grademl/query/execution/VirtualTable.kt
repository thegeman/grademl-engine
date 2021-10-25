package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.model.v2.Table
import science.atlarge.grademl.query.model.v2.TableSchema
import science.atlarge.grademl.query.model.v2.TimeSeriesIterator
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