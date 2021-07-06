package science.atlarge.grademl.query.model

abstract class RowScanner : Iterator<Row> {

    private var prefetchedRow: Row? = null

    protected abstract fun fetchRow(): Row?

    fun peek(): Row? {
        if (prefetchedRow == null) prefetchedRow = fetchRow()
        return prefetchedRow
    }

    override fun hasNext(): Boolean {
        return peek() != null
    }

    override fun next(): Row {
        if (!hasNext()) throw NoSuchElementException()
        val result = prefetchedRow!!
        prefetchedRow = null
        return result
    }

}