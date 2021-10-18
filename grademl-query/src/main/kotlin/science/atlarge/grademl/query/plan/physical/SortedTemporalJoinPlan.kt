package science.atlarge.grademl.query.plan.physical

import science.atlarge.grademl.query.execution.IndexedSortColumn
import science.atlarge.grademl.query.execution.operators.QueryOperator
import science.atlarge.grademl.query.execution.operators.SortedTemporalJoinOperator
import science.atlarge.grademl.query.model.v2.Columns
import science.atlarge.grademl.query.model.v2.TableSchema

class SortedTemporalJoinPlan(
    override val nodeId: Int,
    val leftInput: PhysicalQueryPlan,
    val rightInput: PhysicalQueryPlan,
    val leftJoinColumns: List<IndexedSortColumn>,
    val rightJoinColumns: List<IndexedSortColumn>
) : PhysicalQueryPlan {

    override val schema = TableSchema(
        Columns.RESERVED_COLUMNS +
                leftInput.schema.columns +
                rightInput.schema.columns
    )

    override val children: List<PhysicalQueryPlan>
        get() = listOf(leftInput, rightInput)

    override fun toQueryOperator(): QueryOperator {
        return SortedTemporalJoinOperator(
            leftInput.toQueryOperator(), rightInput.toQueryOperator(),
            schema, leftJoinColumns, rightJoinColumns
        )
    }

}