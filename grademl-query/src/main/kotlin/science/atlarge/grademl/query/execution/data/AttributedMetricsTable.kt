package science.atlarge.grademl.query.execution.data

import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.Column
import science.atlarge.grademl.query.model.ColumnFunction
import science.atlarge.grademl.query.model.RowScanner
import science.atlarge.grademl.query.model.Table

class AttributedMetricsTable : Table {

    override val columns = listOf(
        Column("start_time", "start_time", Type.NUMERIC, ColumnFunction.TIME_START),
        Column("end_time", "end_time", Type.NUMERIC, ColumnFunction.TIME_END),
        Column("duration", "duration", Type.NUMERIC, ColumnFunction.TIME_DURATION),
        Column("utilization", "utilization", Type.NUMERIC, ColumnFunction.OTHER),
        Column("usage", "usage", Type.NUMERIC, ColumnFunction.OTHER),
        Column("capacity", "capacity", Type.NUMERIC, ColumnFunction.OTHER),
        Column("metric_path", "metric_path", Type.STRING, ColumnFunction.OTHER),
        Column("metric_type", "metric_type", Type.STRING, ColumnFunction.OTHER),
        Column("phase_path", "phase_path", Type.STRING, ColumnFunction.OTHER),
        Column("phase_type", "phase_type", Type.STRING, ColumnFunction.OTHER)
    )

    override fun scan(): RowScanner {
        TODO("Not yet implemented")
    }

}