package science.atlarge.grademl.query.plan.logical

import science.atlarge.grademl.query.model.TableSchema

class LimitPlan(
    override val nodeId: Int,
    val input: LogicalQueryPlan,
    val limit: Int
) : LogicalQueryPlan {

    override val schema: TableSchema
        get() = input.schema

    override val children: List<LogicalQueryPlan>
        get() = listOf(input)

    override fun accept(logicalQueryPlanVisitor: LogicalQueryPlanVisitor) {
        logicalQueryPlanVisitor.visit(this)
    }

}