package science.atlarge.grademl.query.model

interface RowScanner {
    fun nextRow(): Row?
}

interface Row {
    fun readBoolean(columnId: Int): Boolean
    fun readNumeric(columnId: Int): Double
    fun readString(columnId: Int): String
}