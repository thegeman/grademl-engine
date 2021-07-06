package science.atlarge.grademl.query.analysis

import science.atlarge.grademl.query.language.*

object ASTUtils {

    fun findColumnLiterals(e: Expression): List<ColumnLiteral> {
        val columnLiterals = mutableListOf<ColumnLiteral>()
        val visitor = object : ExpressionVisitor {
            override fun visit(e: BooleanLiteral) {}
            override fun visit(e: NumericLiteral) {}
            override fun visit(e: StringLiteral) {}

            override fun visit(e: ColumnLiteral) {
                columnLiterals.add(e)
            }

            override fun visit(e: UnaryExpression) {
                e.expr.accept(this)
            }

            override fun visit(e: BinaryExpression) {
                e.lhs.accept(this)
                e.rhs.accept(this)
            }

            override fun visit(e: FunctionCallExpression) {
                e.arguments.forEach { it.accept(this) }
            }

            override fun visit(e: CustomExpression) {
                e.arguments.forEach { it.accept(this) }
            }
        }
        e.accept(visitor)
        return columnLiterals
    }

}