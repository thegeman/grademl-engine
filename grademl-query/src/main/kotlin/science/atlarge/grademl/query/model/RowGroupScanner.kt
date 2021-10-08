package science.atlarge.grademl.query.model

import science.atlarge.grademl.query.nextOrNull

abstract class RowGroupScanner : Iterator<RowGroup> {

    private var prefetchedRowGroup: RowGroup? = null

    protected abstract fun fetchRowGroup(): RowGroup?

    override fun hasNext(): Boolean {
        if (prefetchedRowGroup != null) return true
        prefetchedRowGroup = fetchRowGroup()
        return prefetchedRowGroup != null
    }

    override fun next(): RowGroup {
        if (!hasNext()) throw NoSuchElementException()
        val result = prefetchedRowGroup!!
        prefetchedRowGroup = null
        return result
    }

    fun asRowScanner(): RowScanner {
        return object : RowScanner() {
            private var currentGroup: RowGroup? = null
            override fun fetchRow(): Row? {
                if (currentGroup == null) {
                    if (!this@RowGroupScanner.hasNext()) return null
                    currentGroup = this@RowGroupScanner.next()
                }
                if (!currentGroup!!.hasNext()) return null
                return currentGroup!!.next()
            }
        }
    }

    companion object {

        fun from(rowScanner: RowScanner, columns: List<Column>): RowGroupScanner {
            val rowGroup = object : RowGroup() {
                override val columns = columns
                override val groupedColumnIndices = emptyList<Int>()

                override fun readGroupColumnValue(columnId: Int, outValue: TypedValue): TypedValue {
                    throw UnsupportedOperationException()
                }

                override fun fetchRow(): Row? {
                    return rowScanner.nextOrNull()
                }
            }
            return object : RowGroupScanner() {
                private var nextRowGroup: RowGroup? = rowGroup
                override fun fetchRowGroup(): RowGroup? {
                    val result = nextRowGroup
                    nextRowGroup = null
                    return result
                }
            }
        }

    }

}