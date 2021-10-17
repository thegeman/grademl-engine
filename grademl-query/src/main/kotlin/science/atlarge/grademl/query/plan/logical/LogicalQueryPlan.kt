package science.atlarge.grademl.query.plan.logical

import science.atlarge.grademl.query.model.v2.TableSchema
import kotlin.text.StringBuilder

sealed interface LogicalQueryPlan {

    val nodeId: Int

    val schema: TableSchema

    val children: List<LogicalQueryPlan>

    fun accept(logicalQueryPlanVisitor: LogicalQueryPlanVisitor)

}