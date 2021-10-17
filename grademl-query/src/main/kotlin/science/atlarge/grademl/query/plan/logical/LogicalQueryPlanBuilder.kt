package science.atlarge.grademl.query.plan.logical

import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.language.NamedExpression
import science.atlarge.grademl.query.model.v2.Table

class LogicalQueryPlanBuilder {

    private var nextNodeId: Int = 1

    fun filter(input: LogicalQueryPlan, condition: Expression): LogicalQueryPlan {
        val nodeId = nextNodeId++
        return FilterPlan(nodeId, input, condition)
    }

    fun project(input: LogicalQueryPlan, columnExpressions: List<NamedExpression>): LogicalQueryPlan {
        val nodeId = nextNodeId++
        return ProjectPlan(nodeId, input, columnExpressions)
    }

    fun scan(table: Table, tableName: String): LogicalQueryPlan {
        val nodeId = nextNodeId++
        return ScanTablePlan(nodeId, table, tableName)
    }

    fun temporalJoin(leftInput: LogicalQueryPlan, rightInput: LogicalQueryPlan): LogicalQueryPlan {
        val nodeId = nextNodeId++
        return TemporalJoinPlan(nodeId, leftInput, rightInput)
    }

}