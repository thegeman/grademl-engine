package science.atlarge.grademl.query.model.v2

class TableSchema(val columns: List<Column>) {

    val keyColumns: List<Column>
        get() = columns.filter { it.isKey }
    val valueColumns: List<Column>
        get() = columns.filter { !it.isKey }

}