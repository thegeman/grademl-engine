package science.atlarge.grademl.query.model.v2

interface Row {

    val schema: TableSchema

    fun getBoolean(columnIndex: Int): Boolean
    fun getNumeric(columnIndex: Int): Double
    fun getString(columnIndex: Int): String

}