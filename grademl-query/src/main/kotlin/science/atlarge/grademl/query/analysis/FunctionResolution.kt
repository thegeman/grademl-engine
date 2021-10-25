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

            // Lookup the function definition
            val definition = BuiltinFunctions.find { it.functionName.uppercase() == e.functionName.uppercase().trim() }
            requireNotNull(definition) { "Function with name ${e.functionName} not found" }
            definition.checkArgumentCount(e.arguments.size)
            e.functionDefinition = definition
        }

        override fun visit(e: CustomExpression) {
            e.arguments.forEach { it.accept(this) }
        }
    }

}