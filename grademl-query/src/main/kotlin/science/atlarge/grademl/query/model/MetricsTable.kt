package science.atlarge.grademl.query.model

import science.atlarge.grademl.query.language.Type

class MetricsTable : Table {

    override val columns = listOf(
        Column("start_time", "start_time", Type.NUMERIC),
        Column("end_time", "end_time", Type.NUMERIC),
        Column("duration", "duration", Type.NUMERIC),
        Column("utilization", "utilization", Type.NUMERIC),
        Column("usage", "usage", Type.NUMERIC),
        Column("capacity", "capacity", Type.NUMERIC),
        Column("path", "path", Type.STRING),
        Column("type", "type", Type.STRING)
    )

    override fun scan(): RowScanner {
        TODO("Not yet implemented")
    }

}