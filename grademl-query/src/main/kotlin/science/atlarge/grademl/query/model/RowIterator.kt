package science.atlarge.grademl.query.model

interface RowIterator {

    val schema: TableSchema

    val currentRow: Row

    fun loadNext(): Boolean

}