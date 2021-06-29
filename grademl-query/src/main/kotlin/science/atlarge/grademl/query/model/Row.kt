package science.atlarge.grademl.query.model

import java.lang.IndexOutOfBoundsException

interface Row : Iterable<TypedValue> {

    val columnCount: Int

    fun readValue(columnId: Int, outValue: TypedValue): TypedValue

    override fun iterator() = object : Iterator<TypedValue> {
        private var nextIndex = 0
        private val scratch = TypedValue()

        override fun hasNext() = nextIndex < columnCount

        override fun next(): TypedValue {
            return readValue(nextIndex++, scratch)
        }
    }

    object EMPTY : Row {
        override val columnCount: Int = 0

        override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
            throw IndexOutOfBoundsException(columnId)
        }
    }

    class Proxy : Row {
        var baseRow: Row? = null

        override val columnCount: Int
            get() = baseRow!!.columnCount

        override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
            return baseRow!!.readValue(columnId, outValue)
        }
    }

    companion object {
        fun wrap(columns: Array<TypedValue>) = object : Row {
            override val columnCount: Int
                get() = columns.size

            override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
                columns[columnId].copyTo(outValue)
                return outValue
            }
        }

        fun wrap(columns: List<TypedValue>) = object : Row {
            override val columnCount: Int
                get() = columns.size

            override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
                columns[columnId].copyTo(outValue)
                return outValue
            }
        }

        fun concat(rowA: Row, rowB: Row) = object : Row {
            override val columnCount: Int
                get() = rowA.columnCount + rowB.columnCount

            override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
                val rowACount = rowA.columnCount
                return if (columnId >= rowACount) rowB.readValue(columnId - rowACount, outValue)
                else rowA.readValue(columnId, outValue)
            }
        }
    }

}