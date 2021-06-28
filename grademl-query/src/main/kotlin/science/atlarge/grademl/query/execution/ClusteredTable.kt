package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.model.RowScanner
import science.atlarge.grademl.query.model.Table

class ClusteredTable(inputTable: Table, groupingColumns: List<Int>) : Table {

    override val columns = inputTable.columns

    private val distinctGroupingColumns = groupingColumns.distinct()
    private val sortedInputTable = SortedTable(inputTable, distinctGroupingColumns)

    override fun scan(): RowScanner {
        // TODO: Define RowGroupScanner
        return sortedInputTable.scan()
    }

}