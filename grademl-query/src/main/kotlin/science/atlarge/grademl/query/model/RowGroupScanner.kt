package science.atlarge.grademl.query.model

interface RowGroupScanner : RowScanner {
    fun nextRowGroup(): RowGroup?

    fun rowGroupIterator(): Iterator<RowGroup> = object : Iterator<RowGroup> {
        private var upcomingRowGroup: RowGroup? = null
        private fun prefetchRow(): RowGroup? {
            upcomingRowGroup = nextRowGroup()
            return upcomingRowGroup
        }

        override fun hasNext() = (upcomingRowGroup ?: prefetchRow()) != null

        override fun next(): RowGroup {
            val rowGroup = upcomingRowGroup ?: prefetchRow() ?: throw NoSuchElementException()
            upcomingRowGroup = null
            return rowGroup
        }
    }

    override fun nextRow(): Row? {
        return nextRowGroup()?.nextRow()
    }
}

typealias RowGroup = RowScanner