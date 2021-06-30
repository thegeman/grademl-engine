package science.atlarge.grademl.query.model.impl

import science.atlarge.grademl.core.GradeMLJob
import science.atlarge.grademl.core.TimestampNs
import science.atlarge.grademl.core.models.resource.Metric
import science.atlarge.grademl.core.models.resource.MetricDataIterator
import science.atlarge.grademl.query.language.Expression
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.*

class MetricsTable(private val gradeMLJob: GradeMLJob) : Table {

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
        return MetricsTableScanner(
            gradeMLJob.unifiedResourceModel.rootResource.metricsInTree.toList(),
            gradeMLJob.unifiedExecutionModel.rootPhase.startTime
        )
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

private class MetricsTableScanner(private val metrics: List<Metric>, deltaTs: TimestampNs) : RowScanner() {

    private var currentMetricIndex = -1
    private val rowWrapper = MetricsTableRow(deltaTs)

    override fun fetchRow(): Row? {
        if (currentMetricIndex == -1) nextMetric()
        while (currentMetricIndex < metrics.size && !rowWrapper.dataIterator.hasNext) {
            nextMetric()
        }
        if (!rowWrapper.dataIterator.hasNext) return null
        rowWrapper.dataIterator.next()
        return rowWrapper
    }

    private fun nextMetric() {
        currentMetricIndex++
        if (currentMetricIndex < metrics.size) {
            rowWrapper.metric = metrics[currentMetricIndex]
            rowWrapper.dataIterator = rowWrapper.metric.data.iterator()
        }
    }

}

private class MetricsTableRow(
    val deltaTs: TimestampNs
) : Row {

    lateinit var metric: Metric
    lateinit var dataIterator: MetricDataIterator

    override val columnCount: Int
        get() = 8

    override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
        when (columnId) {
            0 -> /* start_time */ outValue.numericValue = (dataIterator.currentStartTime - deltaTs) * (1 / 1e9)
            1 -> /* end_time */ outValue.numericValue = (dataIterator.currentEndTime - deltaTs) * (1 / 1e9)
            2 -> /* duration */ outValue.numericValue =
                (dataIterator.currentEndTime - dataIterator.currentStartTime) * (1 / 1e9)
            3 -> /* utilization */ outValue.numericValue = dataIterator.currentValue / metric.data.maxValue
            4 -> /* usage */ outValue.numericValue = dataIterator.currentValue
            5 -> /* capacity */ outValue.numericValue = metric.data.maxValue
            6 -> /* path */ outValue.stringValue = metric.path.toString()
            7 -> /* type */ outValue.stringValue = metric.type.toString()
            else -> throw IllegalArgumentException()
        }
        return outValue
    }

}
