package science.atlarge.grademl.cli.data

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.core.TimestampNs
import science.atlarge.grademl.core.resources.DoubleMetricData
import science.atlarge.grademl.core.resources.LongMetricData
import science.atlarge.grademl.core.resources.Metric
import java.io.File

object MetricDataWriter {

    const val FILENAME = "metric-data.tsv"

    fun output(outFile: File, selectedMetrics: Iterable<Metric>, cliState: CliState) {
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

                // Skip metrics without data points
                if (metric.data.timestamps.size < 2) {
                    continue
                }

                val timestamps = metric.data.timestamps
                printLine(timestamps[0], "0")
                when (val data = metric.data) {
                    is DoubleMetricData -> {
                        val values = data.values
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
                    is LongMetricData -> {
                        val values = data.values
                        var lastValue = 0L
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
    }

}