package science.atlarge.grademl.query.model.v2

interface RowIterator {

    val schema: TableSchema

    val currentRow: Row

    fun loadNext(): Boolean

}