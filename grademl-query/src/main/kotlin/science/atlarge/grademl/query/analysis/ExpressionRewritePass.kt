package science.atlarge.grademl.query.analysis

import science.atlarge.grademl.query.language.*

abstract class ExpressionRewritePass {

    fun rewriteExpression(e: Expression): Expression {
        val visitor = createVisitor()
        e.accept(visitor)
        return visitor.lastRewritten
    }

    protected abstract fun createVisitor(): Visitor

    protected abstract class Visitor : ExpressionVisitor {

        lateinit var lastRewritten: Expression
        fun Expression.rewrite(): Expression {
            accept(this@Visitor)
            return lastRewritten
        }

        protected open fun rewrite(e: BooleanLiteral): Expression {
            return e
        }

        protected open fun rewrite(e: NumericLiteral): Expression {
            return e
        }

        protected open fun rewrite(e: StringLiteral): Expression {
            return e
        }

        protected open fun rewrite(e: ColumnLiteral): Expression {
            return e
        }

        protected open fun rewrite(e: UnaryExpression): Expression {
            val rewritten = e.expr.rewrite()
            return if (rewritten === e.expr) e else UnaryExpression(rewritten, e.op)
        }

        protected open fun rewrite(e: BinaryExpression): Expression {
            val lRewritten = e.lhs.rewrite()
            val rRewritten = e.rhs.rewrite()
            return if (lRewritten === e.lhs && rRewritten === e.rhs) e
            else BinaryExpression(lRewritten, rRewritten, e.op)
        }

        protected open fun rewrite(e: FunctionCallExpression): Expression {
            val rewrittenArgs = e.arguments.map { it.rewrite() }
            return if (e.arguments.indices.all { i -> rewrittenArgs[i] === e.arguments[i] }) e
            else FunctionCallExpression(e.functionName, rewrittenArgs)
        }

        protected open fun rewrite(e: CustomExpression): Expression {
            val rewrittenArgs = e.arguments.map { it.rewrite() }
            return if (e.arguments.indices.all { i -> rewrittenArgs[i] === e.arguments[i] }) e
            else CustomExpression(rewrittenArgs, e.originalExpression, e.evalFunction)
        }

        override fun visit(e: BooleanLiteral) {
            lastRewritten = rewrite(e)
        }

        override fun visit(e: NumericLiteral) {
            lastRewritten = rewrite(e)
        }

        override fun visit(e: StringLiteral) {
            lastRewritten = rewrite(e)
        }

        override fun visit(e: ColumnLiteral) {
            lastRewritten = rewrite(e)
        }

        override fun visit(e: UnaryExpression) {
            lastRewritten = rewrite(e)
        }

        override fun visit(e: BinaryExpression) {
            lastRewritten = rewrite(e)
        }

        override fun visit(e: FunctionCallExpression) {
            lastRewritten = rewrite(e)
        }

        override fun visit(e: CustomExpression) {
            lastRewritten = rewrite(e)
        }

    }

}