package science.atlarge.grademl.query.analysis

import science.atlarge.grademl.query.language.*

object AggregateFunctionDecomposition {

    fun decompose(expressions: List<Expression>, inputColumnCount: Int): Result {
        val visitor = Visitor(inputColumnCount)
        val rewrittenExpressions = expressions.map { e ->
            e.accept(visitor)
            visitor.rewrittenExpression
        }
        return Result(
            rewrittenExpressions,
            visitor.aggregateFunctions,
            visitor.aggregateFunctionDepths,
            visitor.aggregateFunctionTypes,
            visitor.rewrittenFunctionArguments
        )
    }

    private class Visitor(private val inputColumnCount: Int) : ExpressionVisitor {

        val aggregateFunctions = arrayListOf<FunctionDefinition>()
        val aggregateFunctionDepths = arrayListOf<Int>()
        val aggregateFunctionTypes = arrayListOf<Type>()
        val rewrittenFunctionArguments = arrayListOf<List<Expression>>()
        private var currentDepth = 0

        lateinit var rewrittenExpression: Expression
            private set
        private fun Expression.rewrite(): Expression {
            accept(this@Visitor)
            return rewrittenExpression
        }

        override fun visit(e: BooleanLiteral) { rewrittenExpression = e }
        override fun visit(e: NumericLiteral) { rewrittenExpression = e }
        override fun visit(e: StringLiteral) { rewrittenExpression = e }
        override fun visit(e: ColumnLiteral) { rewrittenExpression = e }

        override fun visit(e: UnaryExpression) {
            val inner = e.expr.rewrite()
            rewrittenExpression = if (inner === e.expr) e else UnaryExpression(inner, e.op).apply { type = e.type }
        }

        override fun visit(e: BinaryExpression) {
            val l = e.lhs.rewrite()
            val r = e.rhs.rewrite()
            rewrittenExpression = if (l === e.lhs && r === e.rhs) {
                e
            } else {
                BinaryExpression(l, r, e.op).apply { type = e.type }
            }
        }

        override fun visit(e: FunctionCallExpression) {
            rewrittenExpression = if (e.functionDefinition.isAggregatingFunction) {
                currentDepth++
                val args = e.arguments.map { it.rewrite() }
                currentDepth--

                val newColumnIndex = inputColumnCount + aggregateFunctions.size
                aggregateFunctions.add(e.functionDefinition)
                aggregateFunctionDepths.add(currentDepth)
                aggregateFunctionTypes.add(e.type)
                rewrittenFunctionArguments.add(args)
                ColumnLiteral("", "__AGG_${e.functionName}_$newColumnIndex").apply {
                    type = e.type
                    columnIndex = newColumnIndex
                }
            } else {
                val args = e.arguments.map { it.rewrite() }
                if (args.indices.all { i -> args[i] === e.arguments[i] }) e
                else FunctionCallExpression(e.functionName, args).apply {
                    type = e.type
                    functionDefinition = e.functionDefinition
                }
            }
        }

    }

    class Result(
        val rewrittenExpressions: List<Expression>,
        val aggregateFunctions: List<FunctionDefinition>,
        val aggregateFunctionDepths: List<Int>,
        val aggregateFunctionTypes: List<Type>,
        val rewrittenFunctionArguments: List<List<Expression>>
    )

}