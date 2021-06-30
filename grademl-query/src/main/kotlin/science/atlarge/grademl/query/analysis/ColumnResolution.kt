package science.atlarge.grademl.query.analysis

import science.atlarge.grademl.query.language.*
import science.atlarge.grademl.query.model.Column

object ColumnResolution {

    fun resolveColumns(expression: Expression, columns: List<Column>) {
        val columnsByPath = columns.mapIndexed { index, column -> column.path to index }.toMap()
        expression.accept(Visitor(columnsByPath))
    }

    private class Visitor(private val columnIndicesByPath: Map<String, Int>) : ExpressionVisitor {
        override fun visit(e: BooleanLiteral) {}
        override fun visit(e: NumericLiteral) {}
        override fun visit(e: StringLiteral) {}

        override fun visit(e: ColumnLiteral) {
            require(e.columnPath in columnIndicesByPath) { "Column with name ${e.columnPath} not found" }
            e.columnIndex = columnIndicesByPath[e.columnPath]!!
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
            e.originalExpression.accept(this)
        }
    }

}