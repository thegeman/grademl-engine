package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.model.Row
import science.atlarge.grademl.query.model.RowIterator
import science.atlarge.grademl.query.model.TableSchema

abstract class AbstractRowIterator(
    override val schema: TableSchema
) : RowIterator, Row {

    override val currentRow: Row
        get() = this

}