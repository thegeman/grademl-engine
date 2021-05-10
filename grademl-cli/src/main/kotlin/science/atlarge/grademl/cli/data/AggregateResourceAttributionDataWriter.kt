package science.atlarge.grademl.cli.data

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.core.TimestampNs
import science.atlarge.grademl.core.execution.ActivePhaseCountIterator
import science.atlarge.grademl.core.execution.ExecutionPhase
import science.atlarge.grademl.core.resources.Metric
import science.atlarge.grademl.core.resources.MetricData
import science.atlarge.grademl.core.resources.sum
import java.io.File

object AggregateResourceAttributionDataWriter {

    const val FILENAME = "aggregate-resource-attribution-data.tsv"

    fun output(
        outFile: File,
        attributionData: Map<Metric, Map<ExecutionPhase, MetricData>>,
        cliState: CliState
    ) {
        // Group attributed resource data by metric and phase type
        val metricDataByMetricAndPhaseType = attributionData.map { (metric, phaseData) ->
            cliState.metricList.metricToIdentifier(metric) to
                    phaseData.entries.groupBy({ it.key.type }, { it.key to it.value })
                        .map { cliState.phaseTypeList.phaseTypeToIdentifier(it.key) to it.value }
                        .sortedBy { it.first }
        }.sortedBy { it.first }

        outFile.bufferedWriter().use { writer ->
            writer.appendLine("metric.id\tphase.type.id\ttimestamp\tvalue\tcount")
            for ((metricId, phaseTypes) in metricDataByMetricAndPhaseType) {
                for ((phaseTypeId, phaseData) in phaseTypes) {
                    fun printLine(timestamp: TimestampNs, value: String, count: String) {
                        writer.apply {
                            append(metricId)
                            append('\t')
                            append(phaseTypeId)
                            append('\t')
                            append(cliState.normalizeTimestamp(timestamp).toString())
                            append('\t')
                            append(value)
                            append('\t')
                            appendLine(count)
                        }
                    }

                    // Aggregate the attributed resource usage for all phases of a given type
                    val aggregatedMetricIterator = phaseData.map { it.second }
                        .sum(phaseData[0].second.maxValue).iterator()
                    // Count activate phases over time
                    val phaseCountIterator = ActivePhaseCountIterator(phaseData.map { it.first })

                    // Prepare data point generation loop
                    aggregatedMetricIterator.next()
                    var metricValue = aggregatedMetricIterator.currentValue
                    var phaseCount = 0
                    while (
                        phaseCountIterator.hasNext() &&
                        phaseCountIterator.peekNextStartTime() <= aggregatedMetricIterator.currentStartTime
                    ) {
                        phaseCount = phaseCountIterator.next().count
                    }

                    // Iterate over the aggregated metric and phase counts in tandem to produce data points
                    printLine(aggregatedMetricIterator.currentStartTime, "0.0", "0")
                    while (true) {
                        // Check if current time period must be split
                        var skipMetricPoint = false
                        while (
                            phaseCountIterator.hasNext() &&
                            phaseCountIterator.peekNextStartTime() <= aggregatedMetricIterator.currentEndTime
                        ) {
                            val elem = phaseCountIterator.next()
                            if (elem.count != phaseCount) {
                                // Only output a data point whenever the metric value or phase count change
                                printLine(elem.startTime, metricValue.toString(), phaseCount.toString())
                                phaseCount = elem.count
                                skipMetricPoint = (elem.startTime == aggregatedMetricIterator.currentEndTime)
                            }
                        }
                        // Output a data point until the end of the metric period, if needed
                        if (aggregatedMetricIterator.hasNext) {
                            aggregatedMetricIterator.next()
                            if (aggregatedMetricIterator.currentValue != metricValue && !skipMetricPoint) {
                                printLine(
                                    aggregatedMetricIterator.currentStartTime,
                                    metricValue.toString(),
                                    phaseCount.toString()
                                )
                            }
                            metricValue = aggregatedMetricIterator.currentValue
                        } else {
                            // Always output a data point at the end of a time series
                            printLine(
                                aggregatedMetricIterator.currentEndTime,
                                metricValue.toString(),
                                phaseCount.toString()
                            )
                            break
                        }
                    }
                }
            }
        }
    }

}