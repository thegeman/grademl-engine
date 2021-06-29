package science.atlarge.grademl.query.model

interface RowScanner : Iterable<Row> {

    fun nextRow(): Row?

    override fun iterator() = object : Iterator<Row> {
        private var upcomingRow: Row? = null
        private fun prefetchRow(): Row? {
            upcomingRow = nextRow()
            return upcomingRow
        }

        override fun hasNext() = (upcomingRow ?: prefetchRow()) != null

        override fun next(): Row {
            val row = upcomingRow ?: prefetchRow() ?: throw NoSuchElementException()
            upcomingRow = null
            return row
        }
    }

}