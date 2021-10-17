package science.atlarge.grademl.query.model.v2

interface TimeSeries {

    val schema: TableSchema

    fun rowIterator(): RowIterator

    fun getBooleanKey(keyColumnIndex: Int): Boolean
    fun getNumericKey(keyColumnIndex: Int): Double
    fun getStringKey(keyColumnIndex: Int): String

}