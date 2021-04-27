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

                val timestamps = metric.data.timestamps
                when (val data = metric.data) {
                    is DoubleMetricData -> {
                        val values = data.values
                        for (index in timestamps.indices) {
                            printLine(timestamps[index], if (index == 0) "0" else values[index - 1].toString())
                        }
                    }
                    is LongMetricData -> {
                        val values = data.values
                        for (index in timestamps.indices) {
                            printLine(timestamps[index], if (index == 0) "0" else values[index - 1].toString())
                        }
                    }
                }
            }
        }
    }

}