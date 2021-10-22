package science.atlarge.grademl.query.plan.physical

import science.atlarge.grademl.query.analysis.ASTAnalysis
import science.atlarge.grademl.query.analysis.ASTUtils
import science.atlarge.grademl.query.execution.operators.ProjectOperator
import science.atlarge.grademl.query.execution.operators.QueryOperator
import science.atlarge.grademl.query.execution.toPhysicalExpression
import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.language.NamedExpression
import science.atlarge.grademl.query.model.v2.Column
import science.atlarge.grademl.query.model.v2.TableSchema

class ProjectPlan(
    override val nodeId: Int,
    val input: PhysicalQueryPlan,
    columnExpressions: List<NamedExpression>
) : PhysicalQueryPlan {

    val columnExpressions: List<Expression>
    val namedColumnExpressions: List<NamedExpression>
    override val schema: TableSchema

    init {
        val newColumnExpressions = mutableListOf<Expression>()
        val newNamedColumnExpressions = mutableListOf<NamedExpression>()
        val newColumns = mutableListOf<Column>()

        // For each column expression provided: analyze the expression, create a named expression, and create a column
        columnExpressions.forEachIndexed { index, columnExpression ->
            val rewrittenExpression = ASTAnalysis.analyzeExpressionV2(columnExpression.expr, input.schema.columns)
            newColumnExpressions.add(rewrittenExpression)
            newNamedColumnExpressions.add(NamedExpression(rewrittenExpression, columnExpression.name))
            // Determine if the new column is a key
            val columnsUsed = ASTUtils.findColumnLiterals(rewrittenExpression)
            val allKeys = columnsUsed.all { input.schema.columns[it.columnIndex].isKey }
            newColumns.add(Column(columnExpression.name, rewrittenExpression.type, allKeys))
        }

        this.columnExpressions = newColumnExpressions
        this.namedColumnExpressions = newNamedColumnExpressions
        this.schema = TableSchema(newColumns)
    }

    override val children: List<PhysicalQueryPlan>
        get() = listOf(input)

    override fun toQueryOperator(): QueryOperator {
        return ProjectOperator(
            input.toQueryOperator(),
            schema,
            columnExpressions.map { it.toPhysicalExpression() }
        )
    }

    override fun <T> accept(visitor: PhysicalQueryPlanVisitor<T>): T {
        return visitor.visit(this)
    }

}