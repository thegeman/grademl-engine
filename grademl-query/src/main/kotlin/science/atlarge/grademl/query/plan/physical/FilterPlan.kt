package science.atlarge.grademl.query.plan.physical

import science.atlarge.grademl.query.analysis.ASTAnalysis
import science.atlarge.grademl.query.execution.BooleanPhysicalExpression
import science.atlarge.grademl.query.execution.operators.FilterOperator
import science.atlarge.grademl.query.execution.operators.QueryOperator
import science.atlarge.grademl.query.execution.toPhysicalExpression
import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.v2.TableSchema

class FilterPlan(
    override val nodeId: Int,
    val input: PhysicalQueryPlan,
    filterCondition: Expression
) : PhysicalQueryPlan {

    val filterCondition = ASTAnalysis.analyzeExpressionV2(filterCondition, input.schema.columns)

    override val schema: TableSchema
        get() = input.schema
    override val children: List<PhysicalQueryPlan>
        get() = listOf(input)

    init {
        require(filterCondition.type == Type.BOOLEAN) { "Filter condition must be a boolean expression" }
    }

    override fun toQueryOperator(): QueryOperator {
        return FilterOperator(
            input.toQueryOperator(),
            filterCondition.toPhysicalExpression() as BooleanPhysicalExpression
        )
    }

}