package science.atlarge.grademl.query.model.v2

class TableSchema(val columns: List<Column>) {

    val keyColumns: List<Column>
        get() = columns.filter { it.isKey }
    val valueColumns: List<Column>
        get() = columns.filter { !it.isKey }

    init {
        require(columns.map { it.identifier }.toSet().size == columns.size) {
            "TableSchema cannot have duplicate column names: [${
                columns.map { it.identifier }.groupBy { it }.filter { it.value.size > 1 }.keys.joinToString()
            }]"
        }
    }

    fun indexOfStartTimeColumn(): Int? {
        return indexOfColumn(Columns.START_TIME)
    }

    fun indexOfEndTimeColumn(): Int? {
        return indexOfColumn(Columns.END_TIME)
    }

    fun indexOfDurationColumn(): Int? {
        return indexOfColumn(Columns.DURATION)
    }

    fun indexOfColumn(column: Column): Int? {
        val index = columns.indexOfFirst { it == column }
        return if (index >= 0) index else null
    }

}