package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.ensureExhaustive
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.*

class ClusteredTable(inputTable: Table, groupingColumns: List<Int>) : Table {

    override val columns = inputTable.columns

    private val distinctGroupingColumns = groupingColumns.distinct()
    private val sortedInputTable = SortedTable(inputTable, distinctGroupingColumns)

    init {
        require(distinctGroupingColumns.isNotEmpty())
    }

    override fun scan(): RowGroupScanner {
        return ClusteredTableScanner(sortedInputTable.scan(), columns.map { it.type }, distinctGroupingColumns)
    }

}

private class ClusteredTableScanner(
    private val baseScanner: RowScanner,
    private val columnTypes: List<Type>,
    private val groupingColumns: List<Int>
) : RowGroupScanner {

    private val columnCount = groupingColumns.size
    private val booleanColumnValues = BooleanArray(columnCount)
    private val numericColumnValues = DoubleArray(columnCount)
    private val stringColumnValues = Array(columnCount) { "" }
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
            when (columnTypes[columnId]) {
                Type.BOOLEAN -> booleanColumnValues[i] = row.readBoolean(columnId)
                Type.NUMERIC -> numericColumnValues[i] = row.readNumeric(columnId)
                Type.STRING -> stringColumnValues[i] = row.readString(columnId)
            }.ensureExhaustive
        }
    }

    private fun isInSameGroup(row: Row): Boolean {
        return (0 until columnCount).all { i ->
            val columnId = groupingColumns[i]
            when (columnTypes[columnId]) {
                Type.BOOLEAN -> booleanColumnValues[i] == row.readBoolean(columnId)
                Type.NUMERIC -> numericColumnValues[i] == row.readNumeric(columnId)
                Type.STRING -> stringColumnValues[i] == row.readString(columnId)
            }
        }
    }

    private inner class ClusteredRowGroup : RowGroup {

        override fun nextRow(): Row? {
            prefetchedRow = baseScanner.nextRow() ?: return null
            if (!isInSameGroup(prefetchedRow!!)) return null
            return prefetchedRow!!
        }

    }

}