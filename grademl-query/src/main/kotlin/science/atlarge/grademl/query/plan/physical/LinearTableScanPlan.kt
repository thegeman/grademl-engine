package science.atlarge.grademl.query.plan.physical

import science.atlarge.grademl.query.execution.operators.LinearTableScanOperator
import science.atlarge.grademl.query.execution.operators.QueryOperator
import science.atlarge.grademl.query.model.v2.Table
import science.atlarge.grademl.query.model.v2.TableSchema

class LinearTableScanPlan(
    override val nodeId: Int,
    val table: Table,
    val tableName: String
) : PhysicalQueryPlan {

    override val schema: TableSchema
        get() = table.schema
    override val children: List<PhysicalQueryPlan>
        get() = emptyList()

    override fun toQueryOperator(): QueryOperator {
        return LinearTableScanOperator(table)
    }

    override fun <T> accept(visitor: PhysicalQueryPlanVisitor<T>): T {
        return visitor.visit(this)
    }

    override fun isEquivalent(other: PhysicalQueryPlan): Boolean {
        if (other !is LinearTableScanPlan) return false
        return tableName == other.tableName
    }

}