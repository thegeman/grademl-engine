package science.atlarge.grademl.query.plan.physical

import science.atlarge.grademl.query.analysis.ASTAnalysis
import science.atlarge.grademl.query.execution.operators.ProjectOperator
import science.atlarge.grademl.query.execution.operators.QueryOperator
import science.atlarge.grademl.query.execution.toPhysicalExpression
import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.model.v2.TableSchema

class ProjectPlan(
    override val nodeId: Int,
    val input: PhysicalQueryPlan,
    columnExpressions: List<Expression>
) : PhysicalQueryPlan {

    val columnExpressions = columnExpressions.map { ASTAnalysis.analyzeExpressionV2(it, input.schema.columns) }

    override val schema: TableSchema
        get() = input.schema
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