package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.ensureExhaustive
import science.atlarge.grademl.query.language.*
import science.atlarge.grademl.query.model.Row

object ExpressionEvaluation {

    private val visitor = Visitor()

    fun evaluateAsBoolean(e: Expression, row: Row): Boolean {
        require(e.type == Type.BOOLEAN) { "Cannot evaluate ${e.type} expression as BOOLEAN" }
        visitor.row = row
        return visitor.evaluateAsBoolean(e)
    }

    fun evaluateAsNumeric(e: Expression, row: Row): Double {
        require(e.type == Type.NUMERIC) { "Cannot evaluate ${e.type} expression as NUMERIC" }
        visitor.row = row
        return visitor.evaluateAsNumeric(e)
    }

    fun evaluateAsString(e: Expression, row: Row): String {
        require(e.type == Type.STRING) { "Cannot evaluate ${e.type} expression as STRING" }
        visitor.row = row
        return visitor.evaluateAsString(e)
    }

    private class Visitor : ExpressionVisitor {

        lateinit var row: Row

        private var booleanValue = false
        private var numericValue = 0.0
        private var stringValue = ""

        fun evaluateAsBoolean(e: Expression): Boolean {
            e.accept(this)
            return booleanValue
        }

        fun evaluateAsNumeric(e: Expression): Double {
            e.accept(this)
            return numericValue
        }

        fun evaluateAsString(e: Expression): String {
            e.accept(this)
            return stringValue
        }

        override fun visit(e: BooleanLiteral) {
            booleanValue = e.value
        }

        override fun visit(e: NumericLiteral) {
            numericValue = e.value
        }

        override fun visit(e: StringLiteral) {
            stringValue = e.value
        }

        override fun visit(e: ColumnLiteral) {
            when (e.type) {
                Type.BOOLEAN -> booleanValue = row.readBoolean(e.columnIndex)
                Type.NUMERIC -> numericValue = row.readNumeric(e.columnIndex)
                Type.STRING -> stringValue = row.readString(e.columnIndex)
            }.ensureExhaustive
        }

        override fun visit(e: UnaryExpression) {
            when (e.op) {
                UnaryOp.NOT -> booleanValue = !evaluateAsBoolean(e.expr)
            }.ensureExhaustive
        }

        override fun visit(e: BinaryExpression) {
            when (e.op) {
                BinaryOp.ADD -> numericValue = evaluateAsNumeric(e.lhs) + evaluateAsNumeric(e.rhs)
                BinaryOp.SUBTRACT -> numericValue = evaluateAsNumeric(e.lhs) - evaluateAsNumeric(e.rhs)
                BinaryOp.MULTIPLY -> numericValue = evaluateAsNumeric(e.lhs) * evaluateAsNumeric(e.rhs)
                BinaryOp.DIVIDE -> numericValue = evaluateAsNumeric(e.lhs) / evaluateAsNumeric(e.rhs)
                BinaryOp.AND -> booleanValue = evaluateAsBoolean(e.lhs) && evaluateAsBoolean(e.rhs)
                BinaryOp.OR -> booleanValue = evaluateAsBoolean(e.lhs) || evaluateAsBoolean(e.rhs)
                BinaryOp.EQUAL -> booleanValue = when (e.lhs.type) {
                    Type.BOOLEAN -> evaluateAsBoolean(e.lhs) == evaluateAsBoolean(e.rhs)
                    Type.NUMERIC -> evaluateAsNumeric(e.lhs) == evaluateAsNumeric(e.rhs)
                    Type.STRING -> evaluateAsString(e.lhs) == evaluateAsString(e.rhs)
                }
                BinaryOp.NOT_EQUAL -> booleanValue = when (e.lhs.type) {
                    Type.BOOLEAN -> evaluateAsBoolean(e.lhs) != evaluateAsBoolean(e.rhs)
                    Type.NUMERIC -> evaluateAsNumeric(e.lhs) != evaluateAsNumeric(e.rhs)
                    Type.STRING -> evaluateAsString(e.lhs) != evaluateAsString(e.rhs)
                }
                BinaryOp.APPROX_EQUAL -> booleanValue = matchPathsWithWildcards(
                    evaluateAsString(e.lhs), evaluateAsString(e.rhs))
                BinaryOp.NOT_APPROX_EQUAL -> booleanValue = !matchPathsWithWildcards(
                    evaluateAsString(e.lhs), evaluateAsString(e.rhs))
                BinaryOp.GREATER -> booleanValue = evaluateAsNumeric(e.lhs) > evaluateAsNumeric(e.rhs)
                BinaryOp.GREATER_EQUAL -> booleanValue = evaluateAsNumeric(e.lhs) >= evaluateAsNumeric(e.rhs)
                BinaryOp.SMALLER -> booleanValue = evaluateAsNumeric(e.lhs) < evaluateAsNumeric(e.rhs)
                BinaryOp.SMALLER_EQUAL -> booleanValue = evaluateAsNumeric(e.lhs) <= evaluateAsNumeric(e.rhs)
            }.ensureExhaustive
        }

        private fun matchPathsWithWildcards(l: String, r: String): Boolean {
            return when {
                '*' in l -> {
                    require('*' !in r) { "Approximate matching between two paths with wildcards is not supported" }
                    l.split('*').joinToString(separator = ".*") { Regex.escape(it) }.toRegex().matches(r)
                }
                '*' in r -> matchPathsWithWildcards(r, l)
                else -> r == l
            }
        }

        override fun visit(e: FunctionCallExpression) {
            TODO("Not yet implemented")
        }

    }

}