package science.atlarge.grademl.query.model

import science.atlarge.grademl.query.language.Type

class AttributedMetricsTable : Table(
    listOf(
        Column("start_time", "start_time", Type.NUMERIC),
        Column("end_time", "end_time", Type.NUMERIC),
        Column("duration", "duration", Type.NUMERIC),
        Column("utilization", "utilization", Type.NUMERIC),
        Column("usage", "usage", Type.NUMERIC),
        Column("capacity", "capacity", Type.NUMERIC),
        Column("metric_path", "metric_path", Type.STRING),
        Column("metric_type", "metric_type", Type.STRING),
        Column("phase_path", "phase_path", Type.STRING),
        Column("phase_type", "phase_type", Type.STRING)
    )
)