package science.atlarge.grademl.query.model

abstract class RowScanner : Iterator<Row> {

    private var prefetchedRow: Row? = null

    protected abstract fun fetchRow(): Row?

    override fun hasNext(): Boolean {
        if (prefetchedRow != null) return true
        prefetchedRow = fetchRow()
        return prefetchedRow != null
    }

    override fun next(): Row {
        if (!hasNext()) throw NoSuchElementException()
        val result = prefetchedRow!!
        prefetchedRow = null
        return result
    }

}