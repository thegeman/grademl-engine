package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.ensureExhaustive
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.Row
import science.atlarge.grademl.query.model.RowScanner
import science.atlarge.grademl.query.model.Table

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
    private val booleanColumnValues = (0 until columnCount).map { arrayListOf<Boolean>() }
    private val numericColumnValues = (0 until columnCount).map { arrayListOf<Double>() }
    private val stringColumnValues = (0 until columnCount).map { arrayListOf<String>() }
    private val rowOrderInGroup = arrayListOf<Int>()
    private var indexInGroup = 0
    private var groupSize = 0
    private var prefetchedRow: Row? = null
    private val rowWrapper =
        SortedTableRow(booleanColumnValues, numericColumnValues, stringColumnValues, isPreSortedColumn)

    private val comparator = Comparator { leftRowIndex: Int, rightRowIndex: Int ->
        for (c in columnsToSort) {
            when (columnTypes[c]) {
                Type.UNDEFINED -> continue
                Type.BOOLEAN -> {
                    val lVal = booleanColumnValues[c][leftRowIndex]
                    val rVal = booleanColumnValues[c][rightRowIndex]
                    if (lVal == rVal) continue
                    else if (!lVal) return@Comparator -1
                    else return@Comparator 1
                }
                Type.NUMERIC -> {
                    val lVal = numericColumnValues[c][leftRowIndex]
                    val rVal = numericColumnValues[c][rightRowIndex]
                    val comparison = lVal.compareTo(rVal)
                    if (comparison == 0) continue
                    else return@Comparator comparison
                }
                Type.STRING -> {
                    val lVal = stringColumnValues[c][leftRowIndex]
                    val rVal = stringColumnValues[c][rightRowIndex]
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
        booleanColumnValues.forEach { it.clear() }
        numericColumnValues.forEach { it.clear() }
        stringColumnValues.forEach { it.clear() }
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
                when (columnTypes[i]) {
                    Type.UNDEFINED -> {}
                    Type.BOOLEAN -> booleanColumnValues[i].add(row.readBoolean(i))
                    Type.NUMERIC -> numericColumnValues[i].add(row.readNumeric(i))
                    Type.STRING -> stringColumnValues[i].add(row.readString(i))
                }.ensureExhaustive
            }
        }
        // Fetch next
        groupSize++
        prefetchedRow = baseScanner.nextRow()
    }

    private fun isRowInGroup(row: Row): Boolean {
        return preSortedColumns.all { columnId ->
            when (columnTypes[columnId]) {
                Type.UNDEFINED -> true
                Type.BOOLEAN -> booleanColumnValues[columnId][0] == row.readBoolean(columnId)
                Type.NUMERIC -> numericColumnValues[columnId][0] == row.readNumeric(columnId)
                Type.STRING -> stringColumnValues[columnId][0] == row.readString(columnId)
            }
        }
    }

}

private class SortedTableRow(
    private val booleanColumnValues: List<List<Boolean>>,
    private val numericColumnValues: List<List<Double>>,
    private val stringColumnValues: List<List<String>>,
    private val isPreSortedColumn: List<Boolean>
) : Row {

    var rowId: Int = 0

    override fun readBoolean(columnId: Int): Boolean {
        return if (isPreSortedColumn[columnId]) {
            booleanColumnValues[columnId][0]
        } else {
            booleanColumnValues[columnId][rowId]
        }
    }

    override fun readNumeric(columnId: Int): Double {
        return if (isPreSortedColumn[columnId]) {
            numericColumnValues[columnId][0]
        } else {
            numericColumnValues[columnId][rowId]
        }
    }

    override fun readString(columnId: Int): String {
        return if (isPreSortedColumn[columnId]) {
            stringColumnValues[columnId][0]
        } else {
            stringColumnValues[columnId][rowId]
        }
    }

}