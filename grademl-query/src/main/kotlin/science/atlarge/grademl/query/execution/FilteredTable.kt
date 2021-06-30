package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.*
import science.atlarge.grademl.query.nextOrNull

class FilteredTable(val baseTable: Table, val filterExpression: Expression) : Table {

    override val columns = baseTable.columns
    override val isGrouped: Boolean
        get() = baseTable.isGrouped
    override val supportsPushDownFilters: Boolean
        get() = true
    override val supportsPushDownProjections: Boolean
        get() = baseTable.supportsPushDownProjections
    override val supportsPushDownSort: Boolean
        get() = baseTable.supportsPushDownSort
    override val supportsPushDownGroupBy: Boolean
        get() = baseTable.supportsPushDownGroupBy

    init { require(filterExpression.type == Type.BOOLEAN) }

    override fun scan(): RowScanner {
        return FilteredTableScanner(baseTable.scan(), filterExpression)
    }

    override fun scanGroups(): RowGroupScanner {
        TODO("Not yet implemented")
    }

    override fun tryPushDownFilter(filterExpression: Expression): Table? {
        TODO("Not yet implemented")
    }

    override fun tryPushDownProjection(projectionExpressions: List<Expression>): Table? {
        TODO("Not yet implemented")
    }

    override fun tryPushDownSort(sortColumns: List<Int>): Table? {
        TODO("Not yet implemented")
    }

    override fun tryPushDownGroupBy(groupColumns: List<Int>): Table? {
        TODO("Not yet implemented")
    }
}

private class FilteredTableScanner(val baseScanner: RowScanner, val filterExpression: Expression) : RowScanner() {

    private val scratch = TypedValue()

    override fun fetchRow(): Row? {
        while (true) {
            val inputRow = baseScanner.nextOrNull() ?: return null
            if (ExpressionEvaluation.evaluate(filterExpression, inputRow, scratch).booleanValue) return inputRow
        }
    }

}