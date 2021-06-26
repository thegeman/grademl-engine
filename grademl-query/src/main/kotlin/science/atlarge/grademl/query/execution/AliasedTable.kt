package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.model.Column
import science.atlarge.grademl.query.model.Table

class AliasedTable(val baseTable: Table, val aliasPath: String) : Table {
    override val columns: List<Column> = baseTable.columns.map { c ->
        Column(c.name, if (aliasPath.isEmpty()) c.name else "$aliasPath.${c.name}", c.type)
    }
}