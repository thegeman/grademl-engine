package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.model.Row
import science.atlarge.grademl.query.model.RowIterator
import science.atlarge.grademl.query.model.TableSchema

abstract class AbstractRowIterator(
    final override val schema: TableSchema
) : RowIterator, Row {

    final override val currentRow: Row
        get() = this

}