package science.atlarge.grademl.query.model.impl

import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.Column
import science.atlarge.grademl.query.model.RowScanner
import science.atlarge.grademl.query.model.Table

class AttributedMetricsTable : Table {

    override val columns = listOf(
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

    override val isGrouped: Boolean
        get() = false
    override val supportsPushDownFilters: Boolean
        get() = true
    override val supportsPushDownProjections: Boolean
        get() = true
    override val supportsPushDownSort: Boolean
        get() = true
    override val supportsPushDownGroupBy: Boolean
        get() = true

    override fun scan(): RowScanner {
        TODO("Not yet implemented")
    }

    override fun tryPushDownFilter(filterExpression: Expression): Table? {
        TODO("Not yet implemented")
    }

    override fun tryPushDownProjection(projectionExpressions: List<Expression>): Table? {
        TODO("Not yet implemented")
    }

    override fun tryPushDownSort(sortColumns: List<Int>): Table? {
        TODO("Not yet implemented")
    }

    override fun tryPushDownGroupBy(groupColumns: List<Int>): Table? {
        TODO("Not yet implemented")
    }

}