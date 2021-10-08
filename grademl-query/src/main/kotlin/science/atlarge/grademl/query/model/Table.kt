package science.atlarge.grademl.query.model

import science.atlarge.grademl.query.language.ColumnLiteral
import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.nextOrNull

interface Table {

    val columns: List<Column>

    // Linear table scan
    fun scan(): RowScanner

    // Grouped table scan
    val groupedByColumns: List<Column>
        get() = emptyList()
    fun scanGroups(): RowGroupScanner {
        return object : RowGroupScanner() {
            var used = false
            override fun fetchRowGroup(): RowGroup? {
                if (used) return null
                used = true
                return object : RowGroup() {
                    override val columns: List<Column>
                        get() = this@Table.columns
                    override val groupedColumnIndices: List<Int>
                        get() = emptyList()
                    val baseScanner = this@Table.scan()

                    override fun readGroupColumnValue(columnId: Int, outValue: TypedValue): TypedValue {
                        throw UnsupportedOperationException()
                    }

                    override fun fetchRow(): Row? {
                        return baseScanner.nextOrNull()
                    }
                }
            }
        }
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