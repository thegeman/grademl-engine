package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.model.Column
import science.atlarge.grademl.query.model.Table

class AliasedTable(val baseTable: Table, val aliasPath: String) : Table {

    override val columns: List<Column> = baseTable.columns.map { c ->
        Column(c.name, if (aliasPath.isEmpty()) c.name else "$aliasPath.${c.name}", c.type)
    }

    override val isGrouped: Boolean
        get() = baseTable.isGrouped
    override val supportsPushDownFilters: Boolean
        get() = baseTable.supportsPushDownFilters
    override val supportsPushDownProjections: Boolean
        get() = baseTable.supportsPushDownProjections
    override val supportsPushDownSort: Boolean
        get() = baseTable.supportsPushDownSort
    override val supportsPushDownGroupBy: Boolean
        get() = baseTable.supportsPushDownGroupBy

    override fun scan() = baseTable.scan()
    override fun scanGroups() = baseTable.scanGroups()

    override fun tryPushDownFilter(filterExpression: Expression): Table? {
        val newBaseTable = baseTable.tryPushDownFilter(filterExpression) ?: return null
        return AliasedTable(newBaseTable, aliasPath)
    }

    override fun tryPushDownProjection(projectionExpressions: List<Expression>): Table? {
        val newBaseTable = baseTable.tryPushDownProjection(projectionExpressions) ?: return null
        return AliasedTable(newBaseTable, aliasPath)
    }

    override fun tryPushDownSort(sortColumns: List<Int>): Table? {
        val newBaseTable = baseTable.tryPushDownSort(sortColumns) ?: return null
        return AliasedTable(newBaseTable, aliasPath)
    }

    override fun tryPushDownGroupBy(groupColumns: List<Int>): Table? {
        val newBaseTable = baseTable.tryPushDownGroupBy(groupColumns) ?: return null
        return AliasedTable(newBaseTable, aliasPath)
    }

}