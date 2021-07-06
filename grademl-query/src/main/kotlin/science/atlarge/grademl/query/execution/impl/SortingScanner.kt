package science.atlarge.grademl.query.execution.impl

import science.atlarge.grademl.query.model.Row
import science.atlarge.grademl.query.model.RowScanner
import science.atlarge.grademl.query.model.TypedValue
import science.atlarge.grademl.query.nextOrNull

class SortingScanner(
    private val inputScanner: RowScanner,
    private val columnCount: Int,
    private val sortByColumns: List<Int>,
    preSortedColumns: List<Int>
) : RowScanner() {

    init {
        require(sortByColumns.toSet().size == sortByColumns.size)
        require(preSortedColumns.toSet().size == preSortedColumns.size)
    }

    private val usablePreSortedColumns: List<Int> = run {
        val matchingColumns = arrayListOf<Int>()
        for (i in 0 until minOf(preSortedColumns.size, sortByColumns.size)) {
            if (preSortedColumns[i] == sortByColumns[i]) matchingColumns.add(preSortedColumns[i])
            else break
        }
        matchingColumns
    }

    private val remainingSortColumns = sortByColumns - this.usablePreSortedColumns
    private val remainingInputColumns = (0 until columnCount) - sortByColumns

    private val preSortedColumnCount = this.usablePreSortedColumns.size
    private val additionalSortColumnCount = this.remainingSortColumns.size
    private val additionalInputColumnCount = this.remainingInputColumns.size
    private val notPreSortedColumnCount = additionalSortColumnCount + additionalInputColumnCount

    private val columnRemapping = (0 until columnCount).map { originalId ->
        val preSortedId = this.usablePreSortedColumns.indexOf(originalId)
        if (preSortedId >= 0) preSortedId else {
            val toSortColumnId = this.remainingSortColumns.indexOf(originalId)
            if (toSortColumnId >= 0) toSortColumnId + preSortedColumnCount else {
                remainingInputColumns.indexOf(originalId) + preSortedColumnCount + additionalSortColumnCount
            }
        }
    }

    private var cachedRow: Row? = null
    private val fetchedPreSortedColumnValues = Array(preSortedColumnCount) { TypedValue() }
    private var fetchedRowValues = Array(notPreSortedColumnCount * 16) { TypedValue() }
    private var sortedRowOffsets = arrayListOf<Int>()
    private var fetchedRowCount = 0
    private var nextRowNumber = 0
    private val rowWrapper = RowWrapper()

    private val comparator = Comparator { leftRowOffset: Int, rightRowOffset: Int ->
        for (i in 0 until additionalSortColumnCount) {
            val leftVal = fetchedRowValues[leftRowOffset + i]
            val rightVal = fetchedRowValues[rightRowOffset + i]
            val compResult = leftVal.compareTo(rightVal)
            if (compResult != 0) return@Comparator compResult
        }
        return@Comparator leftRowOffset.compareTo(rightRowOffset)
    }

    override fun fetchRow(): Row? {
        // If there are rows in the current group of fetched rows, return the next one
        if (nextRowNumber < fetchedRowCount) {
            rowWrapper.rowOffset = sortedRowOffsets[nextRowNumber]
            nextRowNumber++
            return rowWrapper
        }
        // If there is no cached row, fetch one to determine if we have reached the end of the input
        if (cachedRow == null) cachedRow = inputScanner.nextOrNull() ?: return null

        // Read the next block of rows and sort them
        // Reset the row counter
        fetchedRowCount = 0
        // Determine the pre-sorted column values from the cached row
        readPreSortedColumnValues()
        // Read rows, check if they match the pre-sorted column values, and collect their values
        while (cachedRow != null && matchesPreSortedColumnValues()) {
            addRowValues()
            cachedRow = inputScanner.nextOrNull()
        }
        // Sort the current block of rows
        sortedRowOffsets = ArrayList(fetchedRowCount)
        for (i in 0 until fetchedRowCount) sortedRowOffsets.add(i * notPreSortedColumnCount)
        sortedRowOffsets.sortWith(comparator)

        // Return the first row in the sorted block
        rowWrapper.rowOffset = sortedRowOffsets[0]
        nextRowNumber = 1
        return rowWrapper
    }

    private fun readPreSortedColumnValues() {
        for (i in usablePreSortedColumns.indices) {
            cachedRow!!.readValue(usablePreSortedColumns[i], fetchedPreSortedColumnValues[i])
        }
    }

    private val scratch = TypedValue()
    private fun matchesPreSortedColumnValues(): Boolean {
        for (i in usablePreSortedColumns.indices) {
            cachedRow!!.readValue(usablePreSortedColumns[i], scratch)
            if (scratch != fetchedPreSortedColumnValues[i]) return false
        }
        return true
    }

    private fun addRowValues() {
        // Extend row value array if needed
        if ((fetchedRowCount + 1) * notPreSortedColumnCount > fetchedRowValues.size) {
            fetchedRowValues = Array(fetchedRowValues.size * 2) { i ->
                if (i < fetchedRowValues.size) fetchedRowValues[i] else TypedValue()
            }
        }
        // Read row values
        val nextRowOffset = fetchedRowCount * notPreSortedColumnCount
        val row = cachedRow!!
        for (i in remainingSortColumns.indices) {
            row.readValue(remainingSortColumns[i], fetchedRowValues[nextRowOffset + i])
        }
        for (i in remainingInputColumns.indices) {
            row.readValue(remainingInputColumns[i], fetchedRowValues[nextRowOffset + additionalSortColumnCount + i])
        }
        fetchedRowCount++
    }

    private inner class RowWrapper : Row {

        var rowOffset = 0

        override val columnCount: Int = this@SortingScanner.columnCount

        override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
            val remappedId = columnRemapping[columnId]
            if (remappedId < preSortedColumnCount) fetchedPreSortedColumnValues[remappedId].copyTo(outValue)
            else fetchedRowValues[rowOffset + remappedId - preSortedColumnCount].copyTo(outValue)
            return outValue
        }

    }

}