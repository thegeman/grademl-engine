package science.atlarge.grademl.query.analysis

import science.atlarge.grademl.query.language.*
import science.atlarge.grademl.query.model.Column

object ColumnResolution {

    fun resolveColumns(expression: Expression, columns: List<Column>) {
        val columnsByPath = columns.mapIndexed { index, column -> column.path to index }.toMap()
        val visitor = object : ExpressionVisitor {
            override fun visit(e: BooleanLiteral) {}
            override fun visit(e: NumericLiteral) {}
            override fun visit(e: StringLiteral) {}

            override fun visit(e: ColumnLiteral) {
                require(e.columnPath in columnsByPath) { "Column with name ${e.columnPath} not found" }
                e.columnIndex = columnsByPath[e.columnPath]!!
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
        }
        expression.accept(visitor)
    }

}