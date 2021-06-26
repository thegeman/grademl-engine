package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.model.Column
import science.atlarge.grademl.query.model.Table

class ProjectedTable(
    val baseTable: Table,
    val columnExpressions: List<Expression>,
    val columnNames: List<String>
) : Table {

    override val columns: List<Column>

    init {
        require(columnExpressions.size == columnNames.size)
        columns = columnNames.mapIndexed { index, columnName ->
            Column(columnName, columnName, columnExpressions[index].type)
        }
    }

}