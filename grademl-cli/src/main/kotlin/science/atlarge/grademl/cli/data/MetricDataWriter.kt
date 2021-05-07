package science.atlarge.grademl.cli.data

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.core.TimestampNs
import science.atlarge.grademl.core.TimestampNsRange
import science.atlarge.grademl.core.resources.Metric
import science.atlarge.grademl.core.resources.MetricData
import java.io.File

object MetricDataWriter {

    const val FILENAME = "metric-data.tsv"

    fun output(
        outFile: File,
        selectedMetrics: Iterable<Metric>,
        cliState: CliState,
        metricDataSelector: (Metric) -> MetricData = Metric::data,
        filterTime: TimestampNsRange? = null
    ) {
        outFile.bufferedWriter().use { writer ->
            writer.appendLine("metric.id\ttimestamp\tvalue")
            val metricsById = selectedMetrics.map { cliState.metricList.metricToIdentifier(it) to it }
                .sortedBy { it.first }
            for ((metricId, metric) in metricsById) {
                fun printLine(timestamp: TimestampNs, value: String) {
                    writer.apply {
                        append(metricId)
                        append('\t')
                        append(cliState.normalizeTimestamp(timestamp).toString())
                        append('\t')
                        appendLine(value)
                    }
                }

                // Get slice of metric data if needed
                val metricData = metricDataSelector(metric).let {
                    if (filterTime != null) it.slice(filterTime.first, filterTime.last)
                    else it
                }

                // Skip metrics without data points
                if (metricData.timestamps.size < 2) {
                    continue
                }

                val timestamps = metricData.timestamps
                printLine(timestamps[0], "0")
                val values = metricData.values
                var lastValue = 0.0
                for (index in values.indices) {
                    val newValue = values[index]
                    if (newValue != lastValue) {
                        // Only output a data point whenever the metric's value changes
                        if (index != 0) printLine(timestamps[index], lastValue.toString())
                        lastValue = newValue
                    }
                }
                // Always output a data point at the end of a time series
                printLine(timestamps.last(), lastValue.toString())
            }
        }
    }
}