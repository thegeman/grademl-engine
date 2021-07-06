package science.atlarge.grademl.query.execution.impl

import science.atlarge.grademl.query.model.Row
import science.atlarge.grademl.query.model.RowScanner
import science.atlarge.grademl.query.model.TypedValue
import science.atlarge.grademl.query.nextOrNull

class RemappingScanner(
    private val baseScanner: RowScanner,
    private val selectedColumnIds: List<Int>
) : RowScanner() {

    private val rowWrapper = RowWrapper()

    override fun fetchRow(): Row? {
        rowWrapper.row = baseScanner.nextOrNull() ?: return null
        return rowWrapper
    }

    private inner class RowWrapper : Row {

        lateinit var row: Row

        override val columnCount: Int = selectedColumnIds.size

        override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
            if (columnId !in selectedColumnIds.indices)
                throw IndexOutOfBoundsException("Column $columnId does not exist: table has $columnCount columns")
            return row.readValue(selectedColumnIds[columnId], outValue)
        }

    }

}