package science.atlarge.grademl.query.analysis

import science.atlarge.grademl.query.language.*

class FilterConditionSeparation private constructor() : ExpressionRewritePass() {

    fun split(e: Expression, availableColumnsPerSplit: List<Set<Int>>): Result {
        require(e.type == Type.BOOLEAN) { "Filter condition must be a BOOLEAN expression" }
        val allAvailableColumns = availableColumnsPerSplit.fold(emptySet<Int>()) { a, b -> a + b }
        require(allAvailableColumns.size == availableColumnsPerSplit.sumOf { it.size }) {
            "Available column splits must not overlap"
        }

        val simplifiedCondition = BooleanSimplification.simplify(e)
        val filterTerms = collectFilterTerms(simplifiedCondition)

        val matchedTermsPerSplit = availableColumnsPerSplit.map { mutableListOf<Expression>() }
        val unmatchedTerms = mutableListOf<Expression>()
        for (term in filterTerms) {
            val matchingSplit = findMatchingSplit(term, availableColumnsPerSplit)
            if (matchingSplit != null) {
                matchedTermsPerSplit[matchingSplit].add(term)
            } else {
                unmatchedTerms.add(term)
            }
        }

        return Result(
            matchedTermsPerSplit.map { mergeExpressions(it) },
            mergeExpressions(unmatchedTerms)
        )
    }

    private fun collectFilterTerms(e: Expression): List<Expression> {
        val termsToCheck = mutableListOf<Expression>()
        val termsFound = mutableListOf<Expression>()
        termsToCheck.add(e)
        while (termsToCheck.isNotEmpty()) {
            val term = termsToCheck.removeLast()
            if (term is BinaryExpression && term.op == BinaryOp.AND) {
                termsToCheck.add(term.lhs)
                termsToCheck.add(term.rhs)
            } else {
                termsFound.add(term)
            }
        }
        return termsFound
    }

    private fun findMatchingSplit(e: Expression, columnSplits: List<Set<Int>>): Int? {
        val columnsInExpression = findColumnLiterals(e).map { it.columnIndex }.toSet()
        val splitWithAllColumns = columnSplits.indexOfFirst { it.containsAll(columnsInExpression) }
        return if (splitWithAllColumns >= 0) splitWithAllColumns else null
    }

    private fun findColumnLiterals(e: Expression): List<ColumnLiteral> {
        val columnLiterals = mutableListOf<ColumnLiteral>()
        object : ExpressionVisitor {
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
        return columnLiterals
    }

    private fun mergeExpressions(expressions: Iterable<Expression>): Expression? {
        var result: Expression? = null
        for (e in expressions) {
            if (result == null) result = e
            else result = BinaryExpression(result, e, BinaryOp.AND).apply { type = Type.BOOLEAN }
        }
        return result
    }

    class Result(
        val filterExpressionPerSplit: List<Expression?>,
        val remainingFilterExpression: Expression?
    )

    companion object {
        fun splitFilterConditionByColumns(e: Expression, availableColumnsPerSplit: List<Set<Int>>): Result {
            return FilterConditionSeparation().split(e, availableColumnsPerSplit)
        }
    }

}