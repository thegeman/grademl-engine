package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.model.*

class ClusteredTable(inputTable: Table, groupingColumns: List<Int>) : Table {

    override val columns = inputTable.columns

    private val distinctGroupingColumns = groupingColumns.distinct()
    private val sortedInputTable = SortedTable(inputTable, distinctGroupingColumns)

    init {
        require(distinctGroupingColumns.isNotEmpty())
    }

    override fun scan(): RowGroupScanner {
        return ClusteredTableScanner(sortedInputTable.scan(), distinctGroupingColumns)
    }

}

private class ClusteredTableScanner(
    private val baseScanner: RowScanner,
    private val groupingColumns: List<Int>
) : RowGroupScanner {

    private val columnCount = groupingColumns.size
    private val columnValues = Array(columnCount) { TypedValue() }
    private var prefetchedRow: Row? = null
    private val rowGroup = ClusteredRowGroup()

    override fun nextRowGroup(): RowGroup? {
        // Find the next row group
        if (prefetchedRow == null) {
            prefetchedRow = baseScanner.nextRow() ?: return null
        } else {
            while (isInSameGroup(prefetchedRow!!)) {
                prefetchedRow = baseScanner.nextRow() ?: return null
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
            columnValues[i] == row.readValue(columnId)
        }
    }

    private inner class ClusteredRowGroup : RowGroup {

        override fun nextRow(): Row? {
            val result = prefetchedRow ?: baseScanner.nextRow() ?: return null
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