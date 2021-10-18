package science.atlarge.grademl.query.plan.physical

import science.atlarge.grademl.query.execution.IndexedSortColumn
import science.atlarge.grademl.query.execution.SortColumn
import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.language.NamedExpression
import science.atlarge.grademl.query.model.v2.Table

class PhysicalQueryPlanBuilder {

    private var nextNodeId: Int = 1

    fun filter(input: PhysicalQueryPlan, condition: Expression): PhysicalQueryPlan {
        val nodeId = nextNodeId++
        return FilterPlan(nodeId, input, condition)
    }

    fun linearScan(table: Table): PhysicalQueryPlan {
        val nodeId = nextNodeId++
        return LinearTableScanPlan(nodeId, table)
    }

    fun project(input: PhysicalQueryPlan, columnExpressions: List<NamedExpression>): PhysicalQueryPlan {
        val nodeId = nextNodeId++
        return ProjectPlan(nodeId, input, columnExpressions)
    }

    fun sortedTemporalJoin(
        leftInput: PhysicalQueryPlan,
        rightInput: PhysicalQueryPlan,
        leftJoinColumns: List<IndexedSortColumn>,
        rightJoinColumns: List<IndexedSortColumn>
    ): PhysicalQueryPlan {
        val nodeId = nextNodeId++
        return SortedTemporalJoinPlan(nodeId, leftInput, rightInput, leftJoinColumns, rightJoinColumns)
    }

    fun sort(input: PhysicalQueryPlan, sortByColumns: List<SortColumn>): PhysicalQueryPlan {
        val nodeId = nextNodeId++
        return SortPlan(nodeId, input, sortByColumns)
    }

}