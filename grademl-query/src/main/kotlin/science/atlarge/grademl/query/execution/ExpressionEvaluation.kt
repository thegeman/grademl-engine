package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.ensureExhaustive
import science.atlarge.grademl.query.language.*
import science.atlarge.grademl.query.model.Row
import science.atlarge.grademl.query.model.TypedValue

object ExpressionEvaluation {

    private val visitor = Visitor()

    fun evaluate(e: Expression, row: Row, outValue: TypedValue): TypedValue {
        visitor.row = row
        return visitor.evaluate(e, outValue)
    }

    private class Visitor : ExpressionVisitor {

        lateinit var row: Row

        private var lastValue = TypedValue()
        private var scratch = arrayListOf(TypedValue(), TypedValue(), TypedValue(), TypedValue())

        private fun Expression.evaluate(): TypedValue {
            accept(this@Visitor)
            return lastValue
        }

        fun evaluate(e: Expression, outValue: TypedValue): TypedValue {
            e.evaluate().copyTo(outValue)
            return outValue
        }

        override fun visit(e: BooleanLiteral) {
            lastValue.booleanValue = e.value
        }

        override fun visit(e: NumericLiteral) {
            lastValue.numericValue = e.value
        }

        override fun visit(e: StringLiteral) {
            lastValue.stringValue = e.value
        }

        override fun visit(e: ColumnLiteral) {
            row.readValue(e.columnIndex, lastValue)
        }

        override fun visit(e: UnaryExpression) {
            when (e.op) {
                UnaryOp.NOT -> lastValue.booleanValue = !e.expr.evaluate().booleanValue
            }.ensureExhaustive
        }

        override fun visit(e: BinaryExpression) {
            e.lhs.evaluate().copyTo(scratch[0])
            when (e.op) {
                BinaryOp.ADD -> lastValue.numericValue = scratch[0].numericValue + e.rhs.evaluate().numericValue
                BinaryOp.SUBTRACT -> lastValue.numericValue = scratch[0].numericValue - e.rhs.evaluate().numericValue
                BinaryOp.MULTIPLY -> lastValue.numericValue = scratch[0].numericValue * e.rhs.evaluate().numericValue
                BinaryOp.DIVIDE -> lastValue.numericValue = scratch[0].numericValue / e.rhs.evaluate().numericValue
                BinaryOp.AND -> lastValue.booleanValue = scratch[0].booleanValue && e.rhs.evaluate().booleanValue
                BinaryOp.OR -> lastValue.booleanValue = scratch[0].booleanValue || e.rhs.evaluate().booleanValue
                BinaryOp.EQUAL -> lastValue.booleanValue = scratch[0] == e.rhs.evaluate()
                BinaryOp.NOT_EQUAL -> lastValue.booleanValue = scratch[0] != e.rhs.evaluate()
                BinaryOp.APPROX_EQUAL -> lastValue.booleanValue = matchPathsWithWildcards(
                    scratch[0].stringValue, e.rhs.evaluate().stringValue
                )
                BinaryOp.NOT_APPROX_EQUAL -> lastValue.booleanValue = !matchPathsWithWildcards(
                    scratch[0].stringValue, e.rhs.evaluate().stringValue
                )
                BinaryOp.GREATER -> lastValue.booleanValue =
                    scratch[0].numericValue > e.rhs.evaluate().numericValue
                BinaryOp.GREATER_EQUAL -> lastValue.booleanValue =
                    scratch[0].numericValue >= e.rhs.evaluate().numericValue
                BinaryOp.SMALLER -> lastValue.booleanValue =
                    scratch[0].numericValue < e.rhs.evaluate().numericValue
                BinaryOp.SMALLER_EQUAL -> lastValue.booleanValue =
                    scratch[0].numericValue <= e.rhs.evaluate().numericValue
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

        override fun visit(e: CustomExpression) {
            while (scratch.size < e.arguments.size) scratch.add(TypedValue())
            for (i in e.arguments.indices) {
                e.arguments[i].evaluate().copyTo(scratch[i])
            }
            e.evalFunction(scratch, lastValue)
        }

    }

}