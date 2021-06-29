package science.atlarge.grademl.query.model

interface Row : Iterable<TypedValue> {

    val columnCount: Int

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