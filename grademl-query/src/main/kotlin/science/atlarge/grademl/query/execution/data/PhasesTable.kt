package science.atlarge.grademl.query.execution.data

import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.Column
import science.atlarge.grademl.query.model.ColumnFunction
import science.atlarge.grademl.query.model.RowScanner
import science.atlarge.grademl.query.model.Table

class PhasesTable : Table {

    override val columns = listOf(
        Column("start_time", "start_time", Type.NUMERIC, ColumnFunction.TIME_START),
        Column("end_time", "end_time", Type.NUMERIC, ColumnFunction.TIME_END),
        Column("duration", "duration", Type.NUMERIC, ColumnFunction.TIME_DURATION),
        Column("path", "path", Type.STRING, ColumnFunction.OTHER),
        Column("type", "type", Type.STRING, ColumnFunction.OTHER)
    )

    override val isGrouped: Boolean
        get() = false

    override fun scan(): RowScanner {
        TODO("Not yet implemented")
    }

}