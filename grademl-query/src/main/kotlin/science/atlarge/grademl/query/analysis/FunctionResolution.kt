package science.atlarge.grademl.query.analysis

import science.atlarge.grademl.query.language.*
import science.atlarge.grademl.query.model.BuiltinFunctions

object FunctionResolution {

    fun resolveFunctionCalls(expression: Expression) {
        expression.accept(Visitor)
    }

    private object Visitor : ExpressionVisitor {
        override fun visit(e: BooleanLiteral) {}
        override fun visit(e: NumericLiteral) {}
        override fun visit(e: StringLiteral) {}

        override fun visit(e: ColumnLiteral) {}

        override fun visit(e: UnaryExpression) {
            e.expr.accept(this)
        }

        override fun visit(e: BinaryExpression) {
            e.lhs.accept(this)
            e.rhs.accept(this)
        }

        override fun visit(e: FunctionCallExpression) {
            e.arguments.forEach { it.accept(this) }
            val def = BuiltinFunctions.find { it.functionName == e.functionName.uppercase().trim() }
            requireNotNull(def) { "Function with name ${e.functionName} not found" }
            def.checkArgumentCount(e.arguments.size)
            e.functionDefinition = def
        }
    }

}