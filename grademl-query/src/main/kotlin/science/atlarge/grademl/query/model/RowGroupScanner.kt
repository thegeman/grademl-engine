package science.atlarge.grademl.query.model

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

}