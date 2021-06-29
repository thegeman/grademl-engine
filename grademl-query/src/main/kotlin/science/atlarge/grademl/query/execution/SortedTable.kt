package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.ensureExhaustive
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.Row
import science.atlarge.grademl.query.model.RowScanner
import science.atlarge.grademl.query.model.Table
import science.atlarge.grademl.query.model.TypedValue

interface SortableTable : Table {
    fun canSortOnColumn(columnId: Int): Boolean
    fun sortedBy(columnIds: List<Int>): Table
}

class SortedTable(private val inputTable: Table, sortColumns: List<Int>) : SortableTable {

    override val columns = inputTable.columns

    private val distinctSortColumns = sortColumns.distinct()
    private val preSortedTable: Table
    private val preSortedColumns: Int

    init {
        require(distinctSortColumns.all { it in columns.indices })
        if (inputTable is SortableTable) {
            val firstNonSortedColumn = distinctSortColumns.indexOfFirst { !inputTable.canSortOnColumn(it) }
            preSortedColumns = if (firstNonSortedColumn >= 0) firstNonSortedColumn else distinctSortColumns.size
            preSortedTable = if (preSortedColumns == 0) inputTable else inputTable.sortedBy(
                distinctSortColumns.take(preSortedColumns)
            )
        } else {
            preSortedColumns = 0
            preSortedTable = inputTable
        }
    }

    override fun scan(): RowScanner {
        return if (preSortedColumns == distinctSortColumns.size) {
            preSortedTable.scan()
        } else {
            SortedTableScanner(
                preSortedTable.scan(),
                columns.map { it.type },
                distinctSortColumns.take(preSortedColumns),
                distinctSortColumns.drop(preSortedColumns)
            )
        }
    }

    override fun canSortOnColumn(columnId: Int): Boolean {
        return columnId in columns.indices
    }

    override fun sortedBy(columnIds: List<Int>): Table {
        return SortedTable(inputTable, columnIds + distinctSortColumns)
    }

}

private class SortedTableScanner(
    private val baseScanner: RowScanner,
    private val columnTypes: List<Type>,
    private val preSortedColumns: List<Int>,
    private val columnsToSort: List<Int>
) : RowScanner {

    private val columnCount = columnTypes.size
    private val isPreSortedColumn = (0 until columnCount).map { i -> i in preSortedColumns }
    private val columnValues = (0 until columnCount).map { arrayListOf<TypedValue>() }
    private val rowOrderInGroup = arrayListOf<Int>()
    private var indexInGroup = 0
    private var groupSize = 0
    private var prefetchedRow: Row? = null
    private val rowWrapper = SortedTableRow(columnValues, isPreSortedColumn)
    private val scratch = TypedValue()

    private val comparator = Comparator { leftRowIndex: Int, rightRowIndex: Int ->
        for (c in columnsToSort) {
            when (columnTypes[c]) {
                Type.UNDEFINED -> {
                }
                Type.BOOLEAN -> {
                    val lVal = columnValues[c][leftRowIndex].booleanValue
                    val rVal = columnValues[c][rightRowIndex].booleanValue
                    if (lVal == rVal) continue
                    else if (!lVal) return@Comparator -1
                    else return@Comparator 1
                }
                Type.NUMERIC -> {
                    val lVal = columnValues[c][leftRowIndex].numericValue
                    val rVal = columnValues[c][rightRowIndex].numericValue
                    val comparison = lVal.compareTo(rVal)
                    if (comparison == 0) continue
                    else return@Comparator comparison
                }
                Type.STRING -> {
                    val lVal = columnValues[c][leftRowIndex].stringValue
                    val rVal = columnValues[c][rightRowIndex].stringValue
                    val comparison = lVal.compareTo(rVal)
                    if (comparison == 0) continue
                    else return@Comparator comparison
                }
            }.ensureExhaustive
        }
        return@Comparator leftRowIndex.compareTo(rightRowIndex)
    }

    override fun nextRow(): Row? {
        if (indexInGroup >= groupSize) {
            sortNextGroup()
            if (groupSize == 0) return null
        }
        rowWrapper.rowId = indexInGroup
        indexInGroup++
        return rowWrapper
    }

    private fun sortNextGroup() {
        // Drop all values cached for the last group
        columnValues.forEach { it.clear() }
        rowOrderInGroup.clear()
        indexInGroup = 0
        groupSize = 0

        // Look at the first row in the next group to determine the value of each pre-sorted column
        if (prefetchedRow == null) prefetchedRow = baseScanner.nextRow() ?: return
        appendAndFetchNextRow()

        // Add all rows with the same values for every pre-sorted column
        while (prefetchedRow != null && isRowInGroup(prefetchedRow!!)) {
            appendAndFetchNextRow()
        }

        // Sort rows within a group
        for (i in 0 until groupSize) rowOrderInGroup.add(i)
        rowOrderInGroup.sortWith(comparator)
    }

    private fun appendAndFetchNextRow() {
        // Append
        val row = prefetchedRow!!
        for (i in 0 until columnCount) {
            if (groupSize == 0 || !isPreSortedColumn[i]) {
                columnValues[i].add(row.readValue(i, TypedValue()))
            }
        }
        // Fetch next
        groupSize++
        prefetchedRow = baseScanner.nextRow()
    }

    private fun isRowInGroup(row: Row): Boolean {
        return preSortedColumns.all { columnId ->
            columnValues[columnId][0] == row.readValue(columnId, scratch)
        }
    }

}

private class SortedTableRow(
    private val columnValues: List<List<TypedValue>>,
    private val isPreSortedColumn: List<Boolean>
) : Row {

    override val columnCount = columnValues.size

    var rowId: Int = 0

    override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
        if (isPreSortedColumn[columnId]) outValue.copyFrom(columnValues[columnId][0])
        else outValue.copyFrom(columnValues[columnId][rowId])
        return outValue
    }

}