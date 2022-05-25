package science.atlarge.grademl.query.plan.physical

import science.atlarge.grademl.query.execution.QueryExecutionStatistics
import science.atlarge.grademl.query.execution.operators.LimitOperator
import science.atlarge.grademl.query.execution.operators.QueryOperator
import science.atlarge.grademl.query.model.TableSchema

class LimitPlan(
    override val nodeId: Int,
    val input: PhysicalQueryPlan,
    val limit: Int
) : PhysicalQueryPlan {

    override val schema: TableSchema
        get() = input.schema
    override val children: List<PhysicalQueryPlan>
        get() = listOf(input)

    override fun toQueryOperator(): QueryOperator {
        lastOperator = LimitOperator(input.toQueryOperator(), limit)
        return lastOperator
    }

    private lateinit var lastOperator: LimitOperator

    override fun collectLastExecutionStatisticsPerOperator(): Map<String, QueryExecutionStatistics> {
        return mapOf("LimitOperator" to lastOperator.collectExecutionStatistics())
    }

    override fun <T> accept(visitor: PhysicalQueryPlanVisitor<T>): T {
        return visitor.visit(this)
    }

    override fun isEquivalent(other: PhysicalQueryPlan): Boolean {
        if (other !is LimitPlan) return false
        if (limit != other.limit) return false
        return input.isEquivalent(other.input)
    }

}