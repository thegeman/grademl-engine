package science.atlarge.grademl.cli.data

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.core.resources.Metric
import java.io.File

object MetricListWriter {

    const val FILENAME = "metric-list.tsv"

    fun output(outFile: File, metrics: Iterable<Metric>, cliState: CliState) {
        outFile.bufferedWriter().use { writer ->
            writer.appendLine("metric.id\tmetric.path\tmax.value")
            val metricsById = metrics.map { cliState.metricList.metricToIdentifier(it) to it }.sortedBy { it.first }
            for ((metricId, metric) in metricsById) {
                writer.apply {
                    append(metricId)
                    append('\t')
                    append(metric.path.toString())
                    append('\t')
                    appendLine(metric.data.maxValue.toString())
                }
            }
        }
    }

}