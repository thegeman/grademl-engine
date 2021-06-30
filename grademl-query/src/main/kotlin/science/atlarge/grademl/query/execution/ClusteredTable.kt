package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.model.*
import science.atlarge.grademl.query.nextOrNull

class ClusteredTable(inputTable: Table, groupingColumns: List<Int>) : Table {

    private val distinctGroupingColumns = groupingColumns.distinct()
    private val sortedInputTable = SortedTable(inputTable, distinctGroupingColumns)

    override val columns = sortedInputTable.columns

    override val isGrouped: Boolean
        get() = true
    override val supportsPushDownFilters: Boolean
        get() = sortedInputTable.supportsPushDownFilters
    override val supportsPushDownProjections: Boolean
        get() = sortedInputTable.supportsPushDownProjections
    override val supportsPushDownSort: Boolean
        get() = true
    override val supportsPushDownGroupBy: Boolean
        get() = true

    init {
        require(distinctGroupingColumns.isNotEmpty())
    }

    override fun scan(): RowScanner {
        return scanGroups().asRowScanner()
    }

    override fun scanGroups(): RowGroupScanner {
        return ClusteredTableScanner(sortedInputTable.scan(), distinctGroupingColumns)
    }

    override fun tryPushDownFilter(filterExpression: Expression): Table? {
        TODO("Not yet implemented")
    }

    override fun tryPushDownProjection(projectionExpressions: List<Expression>): Table? {
        TODO("Not yet implemented")
    }

    override fun tryPushDownSort(sortColumns: List<Int>): Table? {
        TODO("Not yet implemented")
    }

    override fun tryPushDownGroupBy(groupColumns: List<Int>): Table? {
        TODO("Not yet implemented")
    }

}

private class ClusteredTableScanner(
    private val baseScanner: RowScanner,
    private val groupingColumns: List<Int>
) : RowGroupScanner() {

    private val columnCount = groupingColumns.size
    private val columnValues = Array(columnCount) { TypedValue() }
    private var prefetchedRow: Row? = null
    private val rowGroup = ClusteredRowGroup()
    private val scratch = TypedValue()

    override fun fetchRowGroup(): RowGroup? {
        // Find the next row group
        if (prefetchedRow == null) {
            prefetchedRow = baseScanner.nextOrNull() ?: return null
        } else {
            while (isInSameGroup(prefetchedRow!!)) {
                prefetchedRow = baseScanner.nextOrNull() ?: return null
            }
        }

        readGroupColumnsFrom(prefetchedRow!!)
        return rowGroup
    }

    private fun readGroupColumnsFrom(row: Row) {
        for (i in 0 until columnCount) {
            val columnId = groupingColumns[i]
            row.readValue(columnId, columnValues[i])
        }
    }

    private fun isInSameGroup(row: Row): Boolean {
        return (0 until columnCount).all { i ->
            val columnId = groupingColumns[i]
            columnValues[i] == row.readValue(columnId, scratch)
        }
    }

    private inner class ClusteredRowGroup : RowGroup() {

        override fun fetchRow(): Row? {
            val result = prefetchedRow ?: baseScanner.nextOrNull() ?: return null
            return if (isInSameGroup(result)) {
                prefetchedRow = null
                result
            } else {
                prefetchedRow = result
                null
            }
        }

    }

}