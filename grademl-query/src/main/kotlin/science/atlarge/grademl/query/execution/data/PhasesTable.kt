package science.atlarge.grademl.query.execution.data

import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.Column
import science.atlarge.grademl.query.model.RowScanner
import science.atlarge.grademl.query.model.Table

class PhasesTable : Table {

    override val columns = listOf(
        Column("start_time", "start_time", Type.NUMERIC),
        Column("end_time", "end_time", Type.NUMERIC),
        Column("duration", "duration", Type.NUMERIC),
        Column("path", "path", Type.STRING),
        Column("type", "type", Type.STRING)
    )

    override val isGrouped: Boolean
        get() = false

    override fun scan(): RowScanner {
        TODO("Not yet implemented")
    }

}