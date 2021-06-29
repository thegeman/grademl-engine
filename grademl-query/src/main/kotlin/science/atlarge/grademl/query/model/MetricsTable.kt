package science.atlarge.grademl.query.model

import science.atlarge.grademl.core.GradeMLJob
import science.atlarge.grademl.core.TimestampNs
import science.atlarge.grademl.core.models.resource.Metric
import science.atlarge.grademl.core.models.resource.MetricDataIterator
import science.atlarge.grademl.query.language.Type

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

    override fun scan(): RowScanner {
        return MetricsTableScanner(
            gradeMLJob.unifiedResourceModel.rootResource.metricsInTree.toList(),
            gradeMLJob.unifiedExecutionModel.rootPhase.startTime
        )
    }

}

private class MetricsTableScanner(private val metrics: List<Metric>, private val deltaTs: TimestampNs) : RowScanner {

    private var currentMetricIndex = -1
    private var currentIterator: MetricDataIterator? = null
    private var currentRowWrapper: MetricsTableRow? = null

    override fun nextRow(): Row? {
        if (currentIterator == null || !currentIterator!!.hasNext) nextMetric()
        currentIterator?.next()
        return currentRowWrapper
    }

    private fun nextMetric() {
        if (currentMetricIndex + 1 < metrics.size) {
            currentMetricIndex++
            currentIterator = metrics[currentMetricIndex].data.iterator()
            currentRowWrapper = MetricsTableRow(metrics[currentMetricIndex], currentIterator!!, deltaTs)
        } else {
            currentIterator = null
            currentRowWrapper = null
        }
    }

}

private class MetricsTableRow(
    val metric: Metric,
    val dataIterator: MetricDataIterator,
    val deltaTs: TimestampNs
) : Row {

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
