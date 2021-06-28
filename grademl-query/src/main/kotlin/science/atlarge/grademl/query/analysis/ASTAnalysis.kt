package science.atlarge.grademl.query.analysis

import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.model.Column

object ASTAnalysis {

    fun analyzeExpression(expression: Expression, columns: List<Column>): Expression {
        ColumnResolution.resolveColumns(expression, columns)
        FunctionResolution.resolveFunctionCalls(expression)
        TypeChecking.analyzeTypes(expression, columns)
        return expression
    }

}