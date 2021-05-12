package science.atlarge.grademl.cli.data

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.core.TimestampNs
import science.atlarge.grademl.core.execution.ActivePhaseCountIterator
import science.atlarge.grademl.core.execution.ExecutionPhase
import science.atlarge.grademl.core.execution.ExecutionPhaseType
import science.atlarge.grademl.core.resources.Metric
import science.atlarge.grademl.core.resources.MetricData
import java.io.File

object AggregateResourceAttributionDataWriter {

    const val FILENAME = "aggregate-resource-attribution-data.tsv"

    fun output(
        outFile: File,
        aggregateAttributionData: Map<Metric, Map<ExecutionPhaseType, MetricData>>,
        phasesPerType: Map<ExecutionPhaseType, List<ExecutionPhase>>,
        cliState: CliState
    ) {
        // Group attributed resource data by metric and phase type
        val metricDataByMetricAndPhaseType = aggregateAttributionData.map { (metric, phaseTypeData) ->
            cliState.metricList.metricToIdentifier(metric) to
                    phaseTypeData.map { it.key to it.value }
                        .sortedBy { it.first.path }
        }.sortedBy { it.first }

        outFile.bufferedWriter().use { writer ->
            writer.appendLine("metric.id\tphase.type.id\ttimestamp\tvalue\tcount")
            for ((metricId, phaseTypes) in metricDataByMetricAndPhaseType) {
                for ((phaseType, phaseData) in phaseTypes) {
                    val phaseTypeId = cliState.phaseTypeList.phaseTypeToIdentifier(phaseType)
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

                    // Count activate phases over time
                    val aggregateMetricIterator = phaseData.iterator()
                    val phaseCountIterator = ActivePhaseCountIterator(phasesPerType[phaseType]!!)

                    // Prepare data point generation loop
                    aggregateMetricIterator.next()
                    var metricValue = aggregateMetricIterator.currentValue
                    var phaseCount = 0
                    while (
                        phaseCountIterator.hasNext() &&
                        phaseCountIterator.peekNextStartTime() <= aggregateMetricIterator.currentStartTime
                    ) {
                        phaseCount = phaseCountIterator.next().count
                    }

                    // Iterate over the aggregated metric and phase counts in tandem to produce data points
                    printLine(aggregateMetricIterator.currentStartTime, "0.0", "0")
                    while (true) {
                        // Check if current time period must be split
                        var skipMetricPoint = false
                        while (
                            phaseCountIterator.hasNext() &&
                            phaseCountIterator.peekNextStartTime() <= aggregateMetricIterator.currentEndTime
                        ) {
                            val elem = phaseCountIterator.next()
                            if (elem.count != phaseCount) {
                                // Only output a data point whenever the metric value or phase count change
                                printLine(elem.startTime, metricValue.toString(), phaseCount.toString())
                                phaseCount = elem.count
                                skipMetricPoint = (elem.startTime == aggregateMetricIterator.currentEndTime)
                            }
                        }
                        // Output a data point until the end of the metric period, if needed
                        if (aggregateMetricIterator.hasNext) {
                            aggregateMetricIterator.next()
                            if (aggregateMetricIterator.currentValue != metricValue && !skipMetricPoint) {
                                printLine(
                                    aggregateMetricIterator.currentStartTime,
                                    metricValue.toString(),
                                    phaseCount.toString()
                                )
                            }
                            metricValue = aggregateMetricIterator.currentValue
                        } else {
                            // Always output a data point at the end of a time series
                            printLine(
                                aggregateMetricIterator.currentEndTime,
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