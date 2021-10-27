package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.model.Row
import science.atlarge.grademl.query.model.RowIterator
import science.atlarge.grademl.query.model.TableSchema

abstract class AccountingRowIterator(
    final override val schema: TableSchema
) : RowIterator, Row {

    final override val currentRow: Row
        get() = this

    var rowsProduced = 0L
        private set

    fun resetRowsProduced() {
        rowsProduced = 0
    }

    protected abstract fun internalLoadNext(): Boolean

    final override fun loadNext(): Boolean {
        if (!internalLoadNext()) return false
        rowsProduced++
        return true
    }

}