package science.atlarge.grademl.query.model.v2

interface TableTransformation : Table {

    val inputTables: List<Table>

}