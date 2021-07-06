package science.atlarge.grademl.query.analysis

import science.atlarge.grademl.query.language.*

abstract class ExpressionRewritePass(
    private val rewriteOriginalOfCustomExpression: Boolean = false
) : ExpressionVisitor {

    private lateinit var lastRewritten: Expression

    fun rewriteExpression(e: Expression): Expression {
        e.accept(this)
        return lastRewritten
    }

    protected fun Expression.rewrite() = rewriteExpression(this)

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
        val rewrittenOriginalExpression =
            if (rewriteOriginalOfCustomExpression) e.originalExpression.rewrite()
            else e.originalExpression
        val hasChanged = rewrittenOriginalExpression !== e.originalExpression ||
                e.arguments.indices.any { i -> rewrittenArgs[i] !== e.arguments[i] }
        return if (hasChanged) CustomExpression(rewrittenArgs, rewrittenOriginalExpression, e.evalFunction) else e
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