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

interface Row : Iterable<TypedValue> {
    val columnCount: Int

    fun readBoolean(columnId: Int): Boolean {
        return readValue(columnId).booleanValue
    }
    fun readNumeric(columnId: Int): Double {
        return readValue(columnId).numericValue
    }
    fun readString(columnId: Int): String {
        return readValue(columnId).stringValue
    }

    fun readValue(columnId: Int): TypedValue {
        return readValue(columnId, TypedValue())
    }
    fun readValue(columnId: Int, outValue: TypedValue): TypedValue

    override fun iterator() = object : Iterator<TypedValue> {
        private var nextIndex = 0

        override fun hasNext() = nextIndex < columnCount

        override fun next(): TypedValue {
            return readValue(nextIndex++)
        }
    }
}