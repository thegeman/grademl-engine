package science.atlarge.grademl.query.analysis

import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.model.Column

object ASTAnalysis {

    fun analyzeExpression(expression: Expression, columns: List<Column>): Expression {
        val clonedExpression = expression.clone()
        ColumnResolution.resolveColumns(clonedExpression, columns)
        FunctionResolution.resolveFunctionCalls(clonedExpression)
        TypeChecking.analyzeTypes(clonedExpression, columns)
        return PatternMatchCompilationPass.rewrite(clonedExpression)
    }

    fun analyzeExpressionV2(expression: Expression, columns: List<science.atlarge.grademl.query.model.v2.Column>): Expression {
        val clonedExpression = expression.clone()
        ColumnResolution.resolveColumns(clonedExpression, columns.mapIndexed { index, column ->
            Column(column.identifier.split('.').last(), column.identifier, index, column.type, column.isKey)
        })
        FunctionResolution.resolveFunctionCalls(clonedExpression)
        TypeChecking.analyzeTypes(clonedExpression, columns.mapIndexed { index, column ->
            Column(column.identifier.split('.').last(), column.identifier, index, column.type, column.isKey)
        })
        return PatternMatchCompilationPass.rewrite(clonedExpression)
    }

}