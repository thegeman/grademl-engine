package science.atlarge.grademl.query.model

import science.atlarge.grademl.query.language.ColumnLiteral
import science.atlarge.grademl.query.language.Expression

interface Table {

    val columns: List<Column>

    // Linear table scan
    fun scan(): RowScanner

    // Grouped table scan
    val isGrouped: Boolean
    fun scanGroups(): RowGroupScanner {
        throw UnsupportedOperationException()
    }

    // Optimizing transformations
    fun withSubsetColumns(subsetColumns: List<ColumnLiteral>): Table? {
        return null
    }

    val columnsOptimizedForFilter: List<Column>
        get() = emptyList()
    fun filteredWith(condition: Expression): Table? {
        return null
    }

    val columnsOptimizedForSort: List<Column>
        get() = emptyList()
    fun sortedBy(sortColumns: List<ColumnLiteral>): Table? {
        return null
    }

}