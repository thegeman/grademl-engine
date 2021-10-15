package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.analysis.ASTAnalysis
import science.atlarge.grademl.query.analysis.ASTUtils
import science.atlarge.grademl.query.execution.scanners.*
import science.atlarge.grademl.query.language.ColumnLiteral
import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.*

class DerivedTable private constructor(
    private val baseTable: Table,
    private val preSortedColumns: List<ColumnLiteral>,
    private val filterCondition: Expression?,
    private val groupByColumns: List<ColumnLiteral>,
    projections: List<Pair<Column, Expression>>,
    private val sortByColumns: List<SortColumn>
) : Table {

    // Filter implementation
    private val filterResultValue = TypedValue()

    private fun rowMatches(row: Row): Boolean {
        return if (filterCondition == null) true
        else ExpressionEvaluation.evaluate(filterCondition, row, filterResultValue).booleanValue
    }

    private fun internalFilteredScanner(): RowScanner = FilteringScanner(baseTable.scan(), this::rowMatches)

    // Group-by implementation
    private fun internalGroupScanner(): RowGroupScanner {
        if (groupByColumns.isEmpty()) throw UnsupportedOperationException()
        val groupColumnIndices = groupByColumns.map { it.columnIndex }
        val preSortedColumnIndices = preSortedColumns.map { it.columnIndex }
        val sortingScanner = SortingScanner(
            internalFilteredScanner(),
            baseTable.columns.size,
            sortByColumns = groupColumnIndices.map { IndexedSortColumn(it, true) },
            preSortedColumns = preSortedColumnIndices.map { IndexedSortColumn(it, true) }
        )
        return GroupingScanner(sortingScanner, baseTable.columns, groupColumnIndices)
    }

    // Projection implementation
    private val projectionExpressions = projections.map { it.second }
    override val columns: List<Column> = projections.map { it.first }

    init {
        require(projectionExpressions.isNotEmpty()) { "Must project at least one output column" }
    }

    private fun internalProjectionScanner(): RowScanner {
        return if (groupByColumns.isNotEmpty()) {
            ProjectingScanner(
                baseGroupScanner = { internalGroupScanner() },
                columnExpressions = projectionExpressions,
                inputColumns = baseTable.columns
            )
        } else {
            ProjectingScanner(
                baseScanner = { internalFilteredScanner() },
                columnExpressions = projectionExpressions,
                inputColumns = baseTable.columns
            )
        }
    }

    private fun internalIntervalMergingScanner(): RowScanner {
        val startTimeColumn = columns.indexOfFirst { it.path == "_start_time" }
        val endTimeColumn = columns.indexOfFirst { it.path == "_end_time" }
        val inputScanner = internalProjectionScanner()
        return if (startTimeColumn >= 0 && endTimeColumn >= 0) {
            IntervalMergingScanner(inputScanner, columns.size, startTimeColumn, endTimeColumn)
        } else {
            inputScanner
        }
    }

    // Sort implementation
    private fun internalSortedScanner(): RowScanner {
        val sortColumnIndices = sortByColumns.map { IndexedSortColumn(it.column.columnIndex, it.ascending) }
        return if (sortByColumns.isNotEmpty()) {
            SortingScanner(internalIntervalMergingScanner(), columns.size, sortColumnIndices, emptyList())
        } else {
            internalIntervalMergingScanner()
        }
    }

    override fun withSubsetColumns(subsetColumns: List<ColumnLiteral>): Table? {
        return super.withSubsetColumns(subsetColumns)
    }

    override fun scan(): RowScanner {
        return internalSortedScanner()
    }

    companion object {

        fun from(
            baseTable: Table,
            filterCondition: Expression?,
            groupByColumns: List<ColumnLiteral>,
            projections: List<Pair<Column, Expression>>,
            sortByColumns: List<SortColumn>
        ): DerivedTable {
            var optimizedBaseTable = baseTable

            // Sanity check filter condition
            require(filterCondition == null || filterCondition.type == Type.BOOLEAN) {
                "Filter condition expression must return a BOOLEAN value"
            }
            // Optimize by pushing filters down if possible
            val filteredTable = applyFilter(optimizedBaseTable, filterCondition)
            if (filteredTable != null) optimizedBaseTable = filteredTable

            // Optimize by selecting only required columns if possible
            val preSelectedTable = preSelectColumns(
                optimizedBaseTable, groupByColumns + projections.map { it.second } +
                        if (filteredTable != null || filterCondition == null) emptyList() else listOf(filterCondition)
            )
            if (preSelectedTable != null) {
                optimizedBaseTable = preSelectedTable
            }

            // Analyze filter condition
            val analyzedFilterCondition = if (filteredTable != null || filterCondition == null) null else
                ASTAnalysis.analyzeExpression(filterCondition, optimizedBaseTable.columns)

            // Analyze and sanity check group-by columns
            val analyzedGroupByColumns = groupByColumns.map {
                ASTAnalysis.analyzeExpression(it, optimizedBaseTable.columns) as ColumnLiteral
            }
            // Optimize by pushing sort for group by down if possible
            val preSortResult = applyPreSortForGroupBy(optimizedBaseTable, analyzedGroupByColumns)
            val preSortedColumns = preSortResult?.second.orEmpty()
            if (preSortResult != null) optimizedBaseTable = preSortResult.first

            // Analyze and sanity check projection expressions
            require(projections.isNotEmpty()) { "Must project at least one output column" }
            val analyzedProjections = projections.map { (column, projectionExpression) ->
                column to ASTAnalysis.analyzeExpression(projectionExpression, optimizedBaseTable.columns)
            }

            // Analyze and sanity check sort-by columns
            val projectedColumns = analyzedProjections.map { it.first }
            val analyzedSortByColumns = sortByColumns.map {
                val analyzedLit = ASTAnalysis.analyzeExpression(it.column, projectedColumns) as ColumnLiteral
                SortColumn(analyzedLit, it.ascending)
            }

            return DerivedTable(
                optimizedBaseTable,
                preSortedColumns,
                analyzedFilterCondition,
                analyzedGroupByColumns,
                analyzedProjections,
                analyzedSortByColumns
            )
        }

        private fun applyFilter(table: Table, filterCondition: Expression?): Table? {
            if (filterCondition != null) {
                val filteredTable = table.filteredWith(filterCondition)
                if (filteredTable != null) {
                    return filteredTable
                }
            }
            return null
        }

        private fun preSelectColumns(table: Table, expressionsToSupport: List<Expression>): Table? {
            // Find recursively all column literals
            val allColumnLiterals = expressionsToSupport.flatMap { ASTUtils.findColumnLiterals(it) }
            // Determine which columns are needed
            val columnsToSelect = allColumnLiterals.map { columnLiteral ->
                ASTAnalysis.analyzeExpression(columnLiteral, table.columns) as ColumnLiteral
            }.sortedBy { it.columnIndex }.distinctBy { it.columnIndex }
            // Check if any columns can be dropped
            if (columnsToSelect.size == table.columns.size) return null
            // Try to push down column selection
            return table.withSubsetColumns(columnsToSelect)
        }

        private fun applyPreSortForGroupBy(
            table: Table,
            groupByColumns: List<ColumnLiteral>
        ): Pair<Table, List<ColumnLiteral>>? {
            if (groupByColumns.isNotEmpty()) {
                // Split group-by columns into optimized and unoptimized columns
                val (optimized, unoptimized) = groupByColumns.partition { c ->
                    table.columnsOptimizedForSort.any { it.path == c.columnPath }
                }
                // First try sorting by all columns
                val sortedTable = table.sortedBy((optimized + unoptimized).map { SortColumn(it, true) })
                if (sortedTable != null) {
                    return sortedTable to (optimized + unoptimized)
                }
                // Otherwise, try sorting only by optimized columns
                if (optimized.isNotEmpty()) {
                    val partialSortedTable = table.sortedBy(optimized.map { SortColumn(it, true) })
                    if (partialSortedTable != null) {
                        return partialSortedTable to optimized
                    }
                }
            }
            return null
        }


    }

}