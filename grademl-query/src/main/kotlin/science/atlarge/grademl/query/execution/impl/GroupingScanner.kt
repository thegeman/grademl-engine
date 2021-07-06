package science.atlarge.grademl.query.execution.impl

import science.atlarge.grademl.query.model.*
import science.atlarge.grademl.query.nextOrNull

class GroupingScanner(
    private val sortedInputScanner: RowScanner,
    private val groupByColumns: List<Int>
) : RowGroupScanner() {

    private val columnCount = groupByColumns.size
    private val columnValues = Array(columnCount) { TypedValue() }
    private var cachedRow: Row? = null
    private val rowGroup = GroupByRowGroup()
    private val scratch = TypedValue()

    override fun fetchRowGroup(): RowGroup? {
        // Find the next row group
        while (true) {
            fetchNextRow() ?: return null
            if (cachedRowIsInSameGroup()) useCachedRow()
            else break
        }

        readGroupColumnsFromCachedRow()
        return rowGroup
    }

    private fun fetchNextRow(): Row? {
        if (cachedRow == null) cachedRow = sortedInputScanner.nextOrNull()
        return cachedRow
    }

    private fun useCachedRow(): Row {
        val row = cachedRow!!
        cachedRow = null
        return row
    }

    private fun readGroupColumnsFromCachedRow() {
        for (i in 0 until columnCount) {
            val columnId = groupByColumns[i]
            cachedRow!!.readValue(columnId, columnValues[i])
        }
    }

    private fun cachedRowIsInSameGroup(): Boolean {
        return (0 until columnCount).all { i ->
            val columnId = groupByColumns[i]
            columnValues[i] == cachedRow!!.readValue(columnId, scratch)
        }
    }

    private inner class GroupByRowGroup : RowGroup() {

        override fun fetchRow(): Row? {
            fetchNextRow() ?: return null
            return if (cachedRowIsInSameGroup()) {
                useCachedRow()
            } else {
                null
            }
        }

    }

}