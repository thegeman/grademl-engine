package science.atlarge.grademl.cli.data

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.core.TimestampNs
import science.atlarge.grademl.core.models.execution.ExecutionPhase
import science.atlarge.grademl.core.models.resource.Metric
import science.atlarge.grademl.core.models.resource.MetricData
import java.io.File

object ResourceAttributionDataWriter {

    const val FILENAME = "resource-attribution-data.tsv"

    fun output(
        outFile: File,
        selectedPhases: Set<ExecutionPhase>,
        attributionData: Map<Metric, Map<ExecutionPhase, MetricData>>,
        cliState: CliState
    ) {
        outFile.bufferedWriter().use { writer ->
            writer.appendLine("metric.id\tphase.id\ttimestamp\tvalue")
            val metricsById = attributionData.map { (metric, phases) ->
                cliState.metricList.metricToIdentifier(metric) to phases
            }.sortedBy { it.first }
            for ((metricId, phases) in metricsById) {
                val phasesById = phases.filter { it.key in selectedPhases }.map { (phase, data) ->
                    cliState.phaseList.phaseToIdentifier(phase) to data
                }.sortedBy { it.first }
                for ((phaseId, phaseData) in phasesById) {
                    fun printLine(timestamp: TimestampNs, value: String) {
                        writer.apply {
                            append(metricId)
                            append('\t')
                            append(phaseId)
                            append('\t')
                            append(cliState.time.normalize(timestamp).toString())
                            append('\t')
                            appendLine(value)
                        }
                    }

                    // Skip metrics without data points
                    if (phaseData.timestamps.size < 2) {
                        continue
                    }

                    val timestamps = phaseData.timestamps
                    printLine(timestamps[0], "0")
                    val values = phaseData.values
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

}