package science.atlarge.grademl.query.plan.physical

import science.atlarge.grademl.query.analysis.ASTAnalysis
import science.atlarge.grademl.query.execution.IndexedSortColumn
import science.atlarge.grademl.query.execution.SortColumn
import science.atlarge.grademl.query.execution.operators.QueryOperator
import science.atlarge.grademl.query.execution.operators.SortedTemporalJoinOperator
import science.atlarge.grademl.query.language.ColumnLiteral
import science.atlarge.grademl.query.model.v2.Columns
import science.atlarge.grademl.query.model.v2.TableSchema

class SortedTemporalJoinPlan(
    override val nodeId: Int,
    val leftInput: PhysicalQueryPlan,
    val rightInput: PhysicalQueryPlan,
    leftJoinColumns: List<SortColumn>,
    rightJoinColumns: List<SortColumn>
) : PhysicalQueryPlan {

    val leftJoinColumns = leftJoinColumns.map {
        SortColumn(ASTAnalysis.analyzeExpressionV2(it.column, leftInput.schema.columns) as ColumnLiteral, it.ascending)
    }
    val rightJoinColumns = rightJoinColumns.map {
        SortColumn(ASTAnalysis.analyzeExpressionV2(it.column, rightInput.schema.columns) as ColumnLiteral, it.ascending)
    }

    override val schema = TableSchema(
        Columns.RESERVED_COLUMNS +
                (leftInput.schema.columns - Columns.RESERVED_COLUMNS) +
                (rightInput.schema.columns - Columns.RESERVED_COLUMNS)
    )

    override val children: List<PhysicalQueryPlan>
        get() = listOf(leftInput, rightInput)

    override fun toQueryOperator(): QueryOperator {
        return SortedTemporalJoinOperator(
            leftInput.toQueryOperator(),
            rightInput.toQueryOperator(),
            schema,
            leftJoinColumns.map { IndexedSortColumn(it.column.columnIndex, it.ascending) },
            rightJoinColumns.map { IndexedSortColumn(it.column.columnIndex, it.ascending) }
        )
    }

    override fun <T> accept(visitor: PhysicalQueryPlanVisitor<T>): T {
        return visitor.visit(this)
    }

}