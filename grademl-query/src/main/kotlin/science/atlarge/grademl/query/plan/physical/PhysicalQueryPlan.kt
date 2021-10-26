package science.atlarge.grademl.query.plan.physical

import science.atlarge.grademl.query.execution.operators.QueryOperator
import science.atlarge.grademl.query.model.TableSchema

interface PhysicalQueryPlan {

    val nodeId: Int

    val schema: TableSchema

    val children: List<PhysicalQueryPlan>

    fun toQueryOperator(): QueryOperator

    fun <T> accept(visitor: PhysicalQueryPlanVisitor<T>): T

    fun isEquivalent(other: PhysicalQueryPlan): Boolean

}