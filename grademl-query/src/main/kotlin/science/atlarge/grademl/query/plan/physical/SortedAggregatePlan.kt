package science.atlarge.grademl.query.plan.physical

import science.atlarge.grademl.query.analysis.ASTAnalysis
import science.atlarge.grademl.query.analysis.ASTUtils
import science.atlarge.grademl.query.analysis.AggregateFunctionDecomposition
import science.atlarge.grademl.query.execution.operators.QueryOperator
import science.atlarge.grademl.query.execution.operators.SortedAggregateOperator
import science.atlarge.grademl.query.execution.toPhysicalExpression
import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.language.NamedExpression
import science.atlarge.grademl.query.model.v2.Column
import science.atlarge.grademl.query.model.v2.TableSchema

class SortedAggregatePlan(
    override val nodeId: Int,
    val input: PhysicalQueryPlan,
    val groupByColumns: List<Int>,
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
        // Decompose aggregate expressions into aggregate functions with arguments expression and final projections
        val aggregateDecomposition = AggregateFunctionDecomposition.decompose(
            columnExpressions, input.schema.columns.size
        )
        aggregateDecomposition.aggregateFunctions

        return SortedAggregateOperator(
            input.toQueryOperator(),
            schema,
            groupByColumns,
            aggregateDecomposition.aggregateFunctions,
            aggregateDecomposition.aggregateFunctionTypes,
            aggregateDecomposition.aggregateFunctionArguments.map { it.map(Expression::toPhysicalExpression) },
            aggregateDecomposition.aggregateFunctionArguments.map { it.map(Expression::type) },
            aggregateDecomposition.aggregateColumns,
            aggregateDecomposition.rewrittenExpressions.map(Expression::toPhysicalExpression)
        )
    }

    override fun <T> accept(visitor: PhysicalQueryPlanVisitor<T>): T {
        return visitor.visit(this)
    }

}