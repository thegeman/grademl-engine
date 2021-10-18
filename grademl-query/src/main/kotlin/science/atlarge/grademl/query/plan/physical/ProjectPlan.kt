package science.atlarge.grademl.query.plan.physical

import science.atlarge.grademl.query.analysis.ASTAnalysis
import science.atlarge.grademl.query.analysis.ASTUtils
import science.atlarge.grademl.query.execution.operators.ProjectOperator
import science.atlarge.grademl.query.execution.operators.QueryOperator
import science.atlarge.grademl.query.execution.toPhysicalExpression
import science.atlarge.grademl.query.language.NamedExpression
import science.atlarge.grademl.query.model.v2.Column
import science.atlarge.grademl.query.model.v2.TableSchema

class ProjectPlan(
    override val nodeId: Int,
    val input: PhysicalQueryPlan,
    columnExpressions: List<NamedExpression>
) : PhysicalQueryPlan {

    val columnExpressions = columnExpressions.map { ASTAnalysis.analyzeExpressionV2(it.expr, input.schema.columns) }

    override val schema: TableSchema = TableSchema(columnExpressions.mapIndexed { i, namedExpression ->
        // Find all columns used in the expression
        val columnsUsed = ASTUtils.findColumnLiterals(namedExpression.expr)
        // Determine if all input columns are keys
        val allKeys = columnsUsed.all { input.schema.columns[it.columnIndex].isKey }
        // Create the new column, mark it as a key column iff all inputs are keys
        Column(namedExpression.name, this.columnExpressions[i].type, allKeys)
    })

    override val children: List<PhysicalQueryPlan>
        get() = listOf(input)

    override fun toQueryOperator(): QueryOperator {
        return ProjectOperator(
            input.toQueryOperator(),
            schema,
            columnExpressions.map { it.toPhysicalExpression() }
        )
    }

}