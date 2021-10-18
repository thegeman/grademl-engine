package science.atlarge.grademl.query.plan.physical

import science.atlarge.grademl.query.execution.operators.LinearTableScanOperator
import science.atlarge.grademl.query.execution.operators.QueryOperator
import science.atlarge.grademl.query.model.v2.Table
import science.atlarge.grademl.query.model.v2.TableSchema

class LinearTableScanPlan(
    override val nodeId: Int,
    val table: Table
) : PhysicalQueryPlan {

    override val schema: TableSchema
        get() = table.schema
    override val children: List<PhysicalQueryPlan>
        get() = emptyList()

    override fun toQueryOperator(): QueryOperator {
        return LinearTableScanOperator(table)
    }

}