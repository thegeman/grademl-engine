package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.model.*

class ProjectedTable(
    val baseTable: Table,
    val columnExpressions: List<Expression>,
    columnNames: List<String>
) : Table {

    override val columns: List<Column>

    init {
        require(columnExpressions.size == columnNames.size)
        columns = columnNames.mapIndexed { index, columnName ->
            Column(columnName, columnName, columnExpressions[index].type)
        }
    }

    override fun scan(): RowScanner {
        return ProjectedTableScanner(baseTable.scan(), columnExpressions)
    }

}

private class ProjectedTableScanner(
    private val baseScanner: RowScanner,
    columnExpressions: List<Expression>
) : RowScanner {

    private val rowWrapper = ProjectedTableRow(columnExpressions)

    override fun nextRow(): Row? {
        val nextRow = baseScanner.nextRow() ?: return null
        rowWrapper.inputRow = nextRow
        return rowWrapper
    }

}

private class ProjectedTableRow(private val columnExpressions: List<Expression>) : Row {

    lateinit var inputRow: Row

    override fun readBoolean(columnId: Int): Boolean {
        return ExpressionEvaluation.evaluateAsBoolean(columnExpressions[columnId], inputRow)
    }

    override fun readNumeric(columnId: Int): Double {
        return ExpressionEvaluation.evaluateAsNumeric(columnExpressions[columnId], inputRow)
    }

    override fun readString(columnId: Int): String {
        return ExpressionEvaluation.evaluateAsString(columnExpressions[columnId], inputRow)
    }

}