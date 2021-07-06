package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.analysis.ColumnReplacementPass
import science.atlarge.grademl.query.language.ColumnLiteral
import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.model.Column
import science.atlarge.grademl.query.model.Table

class AliasedTable(val baseTable: Table, val aliasPath: String) : Table {

    override val columns: List<Column> = baseTable.columns.map { c ->
        Column(c.name, if (aliasPath.isEmpty()) c.name else "$aliasPath.${c.name}", c.type)
    }

    override val isGrouped: Boolean
        get() = baseTable.isGrouped

    override fun scan() = baseTable.scan()
    override fun scanGroups() = baseTable.scanGroups()

    override fun withSubsetColumns(subsetColumns: List<ColumnLiteral>): Table? {
        val renamedColumns = subsetColumns.map { convertExpression(it) as ColumnLiteral }
        val subsetBaseTable = baseTable.withSubsetColumns(renamedColumns) ?: return null
        return AliasedTable(subsetBaseTable, aliasPath)
    }

    override val columnsOptimizedForFilter: List<Column>
        get() = baseTable.columnsOptimizedForFilter

    override fun filteredWith(condition: Expression): Table? {
        val conditionWithRenamedColumns = convertExpression(condition)
        val filteredBaseTable = baseTable.filteredWith(conditionWithRenamedColumns) ?: return null
        return AliasedTable(filteredBaseTable, aliasPath)
    }

    override val columnsOptimizedForSort: List<Column>
        get() = baseTable.columnsOptimizedForSort

    override fun sortedBy(sortColumns: List<ColumnLiteral>): Table? {
        val renamedColumns = sortColumns.map { convertExpression(it) as ColumnLiteral }
        val sortedBaseTable = baseTable.sortedBy(renamedColumns) ?: return null
        return AliasedTable(sortedBaseTable, aliasPath)
    }

    private fun convertExpression(expression: Expression): Expression {
        return ColumnReplacementPass.replaceColumnLiterals(expression) { columnLiteral ->
            val matchingColumn = baseTable.columns.find { it.name == columnLiteral.columnName }!!
            ColumnLiteral(matchingColumn.path).also {
                it.columnIndex = columnLiteral.columnIndex
                it.type = columnLiteral.type
            }
        }
    }

}