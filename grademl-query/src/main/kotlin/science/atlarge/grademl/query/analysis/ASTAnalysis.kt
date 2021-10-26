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

}