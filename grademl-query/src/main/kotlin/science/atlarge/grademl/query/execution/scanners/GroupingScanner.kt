package science.atlarge.grademl.query.execution.scanners

import science.atlarge.grademl.query.model.*
import science.atlarge.grademl.query.nextOrNull

class GroupingScanner(
    private val sortedInputScanner: RowScanner,
    private val allColumns: List<Column>,
    private val groupByColumns: List<Int>
) : RowGroupScanner() {

    private val groupByColumnCount = groupByColumns.size
    private val groupByColumnValues = Array(groupByColumnCount) { TypedValue() }
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
        for (i in 0 until groupByColumnCount) {
            val columnId = groupByColumns[i]
            cachedRow!!.readValue(columnId, groupByColumnValues[i])
        }
    }

    private fun cachedRowIsInSameGroup(): Boolean {
        return (0 until groupByColumnCount).all { i ->
            val columnId = groupByColumns[i]
            groupByColumnValues[i] == cachedRow!!.readValue(columnId, scratch)
        }
    }

    private inner class GroupByRowGroup : RowGroup() {

        override val columns = allColumns
        override val groupedColumnIndices = groupByColumns

        private val inverseColumnMap = Array(allColumns.size) { columnId ->
            groupByColumns.indexOf(columnId)
        }

        override fun readGroupColumnValue(columnId: Int, outValue: TypedValue): TypedValue {
            val groupColumnId = inverseColumnMap[columnId]
            groupByColumnValues[groupColumnId].copyTo(outValue)
            return outValue
        }

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