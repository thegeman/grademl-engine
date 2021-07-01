package science.atlarge.grademl.query.analysis

import science.atlarge.grademl.query.language.*
import science.atlarge.grademl.query.model.TypedValue

object PatternMatchCompilationPass {

    fun rewrite(e: Expression): Expression = object : ExpressionRewritePass() {
        override fun rewrite(e: BinaryExpression): Expression {
            if (e.op != BinaryOp.APPROX_EQUAL && e.op != BinaryOp.NOT_APPROX_EQUAL) return super.rewrite(e)
            if (e.rhs !is StringLiteral) return super.rewrite(e)

            val pattern = e.rhs.value
            val regex = pattern.split('*').joinToString(".*") { Regex.escape(it) }.toRegex()
            val matchFn = if (e.op == BinaryOp.APPROX_EQUAL) {
                { args: List<TypedValue>, outValue: TypedValue ->
                    outValue.booleanValue = regex.matches(args[0].stringValue)
                    outValue
                }
            } else {
                { args: List<TypedValue>, outValue: TypedValue ->
                    outValue.booleanValue = !regex.matches(args[0].stringValue)
                    outValue
                }
            }
            return CustomExpression(listOf(e.lhs), e, matchFn).apply { type = Type.BOOLEAN }
        }
    }.rewriteExpression(e)

}