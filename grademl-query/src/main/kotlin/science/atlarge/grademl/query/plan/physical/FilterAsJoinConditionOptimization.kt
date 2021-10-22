package science.atlarge.grademl.query.plan.physical

import science.atlarge.grademl.query.analysis.ASTUtils
import science.atlarge.grademl.query.analysis.FilterConditionSeparation
import science.atlarge.grademl.query.execution.SortColumn
import science.atlarge.grademl.query.language.*
import science.atlarge.grademl.query.model.v2.Column
import science.atlarge.grademl.query.model.v2.Columns

object FilterAsJoinConditionOptimization : OptimizationStrategy, PhysicalQueryPlanRewriter {

    override fun optimize(physicalQueryPlan: PhysicalQueryPlan): PhysicalQueryPlan? {
        return physicalQueryPlan.accept(this)
    }

    override fun visit(filterPlan: FilterPlan): PhysicalQueryPlan? {
        // Match on filters directly after a join
        return when (filterPlan.input) {
            is SortedTemporalJoinPlan -> collapseFilterIntoJoin(filterPlan.filterCondition, filterPlan.input)
            else -> null
        }
    }

    private fun collapseFilterIntoJoin(
        filterCondition: Expression,
        sortedTemporalJoinPlan: SortedTemporalJoinPlan
    ): PhysicalQueryPlan? {
        // Find equality checks in the filter condition that compare one input to the other
        val (leftRightEqualities, remainingFilter) = extractLeftRightEqualityTerms(
            filterCondition,
            sortedTemporalJoinPlan.leftInput.schema.columns.map { it.identifier }.toSet() -
                    Columns.RESERVED_COLUMN_NAMES,
            sortedTemporalJoinPlan.rightInput.schema.columns.map { it.identifier }.toSet() -
                    Columns.RESERVED_COLUMN_NAMES
        )
        // Return early if there are no equalities that can be used as join conditions
        if (leftRightEqualities.isEmpty()) return null
        // Convert all complex expressions into concrete columns on the left and right inputs
        val (newColumns, columnEqualities) = convertLeftRightExpressionsToColumns(leftRightEqualities)
        // Rewrite the left and right inputs with added columns
        var rewrittenLeft = projectInput(sortedTemporalJoinPlan.leftInput, newColumns.first)
        var rewrittenRight = projectInput(sortedTemporalJoinPlan.rightInput, newColumns.second)
        // TODO: Determine equality sets to create more restrictive filters (e.g., for nested joins)
        // Determine the column IDs of left and right join columns
        val leftJoinColumns = columnEqualities.map {
            rewrittenLeft.schema.column(it.first) ?: throw IllegalArgumentException()
        }
        val rightJoinColumns = columnEqualities.map {
            rewrittenRight.schema.column(it.second) ?: throw IllegalArgumentException()
        }
        // Order the join columns to make sorting inputs as efficient as possible
        val (orderedLeftJoinColumns, orderedRightJoinColumns) = orderJoinColumns(leftJoinColumns, rightJoinColumns)
        // Sort the left and right inputs on the common join column order
        rewrittenLeft = PhysicalQueryPlanBuilder.sort(rewrittenLeft, orderedLeftJoinColumns)
        rewrittenRight = PhysicalQueryPlanBuilder.sort(rewrittenRight, orderedRightJoinColumns)
        // Join the sorted inputs
        val newJoin = PhysicalQueryPlanBuilder.sortedTemporalJoin(
            leftInput = rewrittenLeft,
            rightInput = rewrittenRight,
            leftJoinColumns = mergeJoinColumns(orderedLeftJoinColumns, sortedTemporalJoinPlan.leftJoinColumns),
            rightJoinColumns = mergeJoinColumns(orderedRightJoinColumns, sortedTemporalJoinPlan.rightJoinColumns),
            leftDropColumns = newColumns.first.map { it.name }.toSet() + sortedTemporalJoinPlan.leftDropColumns,
            rightDropColumns = newColumns.second.map { it.name }.toSet() + sortedTemporalJoinPlan.rightDropColumns
        )
        // Add an outer filter operation with part of the filter condition that could not be pushed down
        return if (remainingFilter != null) {
            PhysicalQueryPlanBuilder.filter(newJoin, remainingFilter)
        } else {
            newJoin
        }
    }

    private fun extractLeftRightEqualityTerms(
        filterCondition: Expression,
        leftColumns: Set<String>,
        rightColumns: Set<String>
    ): Pair<List<Pair<Expression, Expression>>, Expression?> {
        // Break the filter condition (an AND expression with 0 or more ANDs) into terms
        val terms = FilterConditionSeparation.collectAndExpressionTerms(filterCondition)
        // Find all terms checking for equality
        val (equalityTerms, nonEqualityTerms) = terms.partition { it is BinaryExpression && it.op == BinaryOp.EQUAL }
        // Determine for each equality if it compares an expression on the left join input
        // with an expression on the right join input
        val leftRightEqualityPairs = mutableListOf<Pair<Expression, Expression>>()
        val unmatchedEqualityTerms = mutableListOf<Expression>()
        for (expr in equalityTerms) {
            expr as BinaryExpression
            // Extract column literals from both inputs to the equality check
            val leftTermColumnLiterals = ASTUtils.findColumnLiterals(expr.lhs)
            val rightTermColumnLiterals = ASTUtils.findColumnLiterals(expr.rhs)
            // Determine for each set of literals if they use the left join input, right join input, or both
            val leftUsesOnlyLeft = leftTermColumnLiterals.all { it.columnPath in leftColumns }
            val leftUsesOnlyRight = leftTermColumnLiterals.all { it.columnPath in rightColumns }
            val rightUsesOnlyLeft = rightTermColumnLiterals.all { it.columnPath in leftColumns }
            val rightUsesOnlyRight = rightTermColumnLiterals.all { it.columnPath in rightColumns }
            // Return this equality if both input terms use a different join input
            if (leftUsesOnlyLeft && rightUsesOnlyRight) {
                leftRightEqualityPairs.add(expr.lhs to expr.rhs)
            } else if (leftUsesOnlyRight && rightUsesOnlyLeft) {
                leftRightEqualityPairs.add(expr.rhs to expr.lhs)
            } else {
                unmatchedEqualityTerms.add(expr)
            }
        }
        // Combine any unmatched equality terms into a single AND expression
        val remainingCondition = FilterConditionSeparation.mergeExpressions(
            nonEqualityTerms + unmatchedEqualityTerms
        )
        return leftRightEqualityPairs to remainingCondition
    }

    private fun convertLeftRightExpressionsToColumns(
        leftRightEqualities: List<Pair<Expression, Expression>>
    ): Pair<Pair<List<NamedExpression>, List<NamedExpression>>, List<Pair<String, String>>> {
        val (addedLeftColumns, leftNames) = convertExpressionsToColumns(leftRightEqualities.map { it.first })
        val (addedRightColumns, rightNames) = convertExpressionsToColumns(leftRightEqualities.map { it.second })
        return (addedLeftColumns to addedRightColumns) to leftNames.zip(rightNames)
    }

    private fun convertExpressionsToColumns(
        expressions: List<Expression>
    ): Pair<List<NamedExpression>, List<String>> {
        val addedColumns = mutableListOf<NamedExpression>()
        val columnNames = expressions.map {
            if (it !is ColumnLiteral) {
                val newColumnName = PhysicalQueryPlanBuilder.generateColumnName("__join_")
                addedColumns.add(NamedExpression(it, newColumnName))
                newColumnName
            } else {
                it.columnPath
            }
        }
        return addedColumns to columnNames
    }

    private fun projectInput(input: PhysicalQueryPlan, newColumns: List<NamedExpression>): PhysicalQueryPlan {
        return if (newColumns.isNotEmpty()) {
            val namedInputColumns = input.schema.columns.map {
                NamedExpression(ColumnLiteral(it.identifier), it.identifier)
            }
            PhysicalQueryPlanBuilder.project(input, namedInputColumns + newColumns)
        } else {
            input
        }
    }

    private fun orderJoinColumns(
        leftColumns: List<Column>,
        rightColumns: List<Column>
    ): Pair<List<SortColumn>, List<SortColumn>> {
        // TODO: Find the optimal sort order across both inputs
        // TODO: Add information about how a plan is sorted as a property to PhysicalQueryPlan
        return leftColumns.map {
            SortColumn(ColumnLiteral(it.identifier), true)
        } to rightColumns.map {
            SortColumn(ColumnLiteral(it.identifier), true)
        }
    }

    private fun mergeJoinColumns(
        newJoinColumns: List<SortColumn>,
        oldJoinColumns: List<SortColumn>
    ): List<SortColumn> {
        val sortDirections = mutableMapOf<String, Boolean>()
        return (newJoinColumns + oldJoinColumns).map {
            if (it.column.columnPath in sortDirections) {
                val direction = sortDirections[it.column.columnPath]!!
                if (direction != it.ascending) SortColumn(it.column, direction)
                else it
            } else {
                sortDirections[it.column.columnPath] = it.ascending
                it
            }
        }
    }

}