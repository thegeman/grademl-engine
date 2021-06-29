package science.atlarge.grademl.query.model

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