package science.atlarge.grademl.query.model.impl

import science.atlarge.grademl.query.language.Expression
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