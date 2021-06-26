package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.Row
import science.atlarge.grademl.query.model.RowScanner
import science.atlarge.grademl.query.model.Table

class FilteredTable(val baseTable: Table, val filterExpression: Expression) : Table {

    override val columns = baseTable.columns

    init { require(filterExpression.type == Type.BOOLEAN) }

    override fun scan(): RowScanner {
        return FilteredTableScanner(baseTable.scan(), filterExpression)
    }

}

private class FilteredTableScanner(val baseScanner: RowScanner, val filterExpression: Expression) : RowScanner {

    override fun nextRow(): Row? {
        while (true) {
            val inputRow = baseScanner.nextRow() ?: return null
            if (ExpressionEvaluation.evaluateAsBoolean(filterExpression, inputRow)) return inputRow
        }
    }

}