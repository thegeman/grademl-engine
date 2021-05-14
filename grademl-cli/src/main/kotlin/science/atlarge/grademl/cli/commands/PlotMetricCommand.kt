package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.data.*
import science.atlarge.grademl.cli.terminal.*
import science.atlarge.grademl.cli.util.parseMetricPathExpression
import science.atlarge.grademl.cli.util.tryMatchMetricPath
import science.atlarge.grademl.core.TimestampNs
import science.atlarge.grademl.core.TimestampNsRange
import science.atlarge.grademl.core.attribution.AttributedResourceData
import science.atlarge.grademl.core.attribution.NoAttributedData
import science.atlarge.grademl.core.models.resource.Metric
import science.atlarge.grademl.core.models.resource.sum
import science.atlarge.grademl.core.util.asRPathString
import science.atlarge.grademl.core.util.instantiateRScript
import science.atlarge.grademl.core.util.runRScript

object PlotMetricCommand : Command(
    name = "plot-metric",
    shortHelpMessage = "plot a comprehensive view of a particular metric",
    longHelpMessage = "Plots a metric's observed time series, its upsampled time series, " +
            "and its attribution to phases.",
    supportedOptions = listOf(
        Option(
            null,
            "show-phases",
            "show phases instead of phase types in plot"
        ),
        Option(
            null,
            "zoom-time",
            "zoom into a specific period of time, formatted as 'start:end', in seconds",
            argument = OptionArgument(
                "START:END"
            )
        )
    ),
    supportedArguments = listOf(
        Argument(
            "metric_path",
            "path of metric to plot",
            ArgumentValueConstraint.MetricPath,
            isOptional = false,
            isVararg = true
        )
    )
) {

    private const val PLOT_FILENAME = "metric-overview.pdf"
    private const val SCRIPT_FILENAME = "plot-metric.R"

    override fun process(parsedCommand: ParsedCommand, cliState: CliState) {
        // Parse options
        val showPhases = parsedCommand.isOptionProvided("show-phases")
        val zoomTime = parsedCommand.getOptionValue("zoom-time")?.let { arg ->
            parseZoomTimeArgument(arg, cliState.time::denormalize) ?: return
        }

        // Parse the metric_path argument value(s) for a set of metrics to plot
        val metrics = parsedCommand.getArgumentValues("metric_path")
            .map { parseMetricPathExpression(it) ?: return }
            .map { tryMatchMetricPath(it, cliState) ?: return }
            .flatten()
            .toSet()

        // Plot each metric
        var isFirst = true
        for (metric in metrics) {
            if (!isFirst) println()
            isFirst = false
            println("Plotting overview for metric: \"${metric.path}\".")
            plotMetric(metric, showPhases, zoomTime, cliState)
        }
    }

    private fun parseZoomTimeArgument(argument: String, denormalizeTs: (Long) -> TimestampNs): TimestampNsRange? {
        // Try extracting two strings representing decimal numbers from the argument
        val regex = """(-?[0-9]+(?:\.[0-9]+)?):(-?[0-9]+(?:\.[0-9]+)?)""".toRegex()
        val match = regex.matchEntire(argument)
        if (match == null) {
            println("Failed to parse zoom-time argument:")
            println("  Argument does not match expected format of START:END, where START and END are decimal numbers.")
            return null
        }
        // Define a helper function for parsing a decimal number of seconds to an integer number of nanoseconds
        fun parseDecimalToTimestamp(s: String): TimestampNs? {
            val intLimit = Long.MAX_VALUE / 1_000_000_000
            val (intPart, decimalPart) = if ('.' in s) {
                val splits = s.split('.')
                splits[0] to splits[1].trimEnd('0')
            } else {
                s to ""
            }
            val intPartValue = intPart.toLongOrNull()
            if (intPartValue == null || intPartValue > intLimit || intPartValue < -intLimit) {
                println("Failed to parse zoom-time argument:")
                println("  Integer part of given value $s exceeds the timestamp limits.")
                return null
            }
            if (decimalPart.length > 9) {
                println("Failed to parse zoom-time argument:")
                println("  Decimal part of given value $s exceeds the supported nanosecond precision.")
                return null
            }
            val decimalPartValue = decimalPart.padEnd(9, '0').toLong()
            return intPartValue * 1_000_000_000 + if (intPartValue >= 0) decimalPartValue else -decimalPartValue
        }
        // Parse the start and end timestamps
        val normalizedStartTime = parseDecimalToTimestamp(match.groupValues[1]) ?: return null
        val normalizedEndTime = parseDecimalToTimestamp(match.groupValues[2]) ?: return null
        if (normalizedEndTime < normalizedStartTime) {
            println("Failed to parse zoom-time argument:")
            println(
                "  The given end time (${match.groupValues[2]}) is earlier than " +
                        "the given start time (${match.groupValues[1]})."
            )
            return null
        }
        // Convert normalized timestamps to unix timestamps
        val startTime = denormalizeTs(normalizedStartTime)
        if (startTime < normalizedStartTime) {
            println("Failed to parse zoom-time argument:")
            println("  The given start time (${match.groupValues[1]}) exceeds the timestamp limits.")
            return null
        }
        val endTime = denormalizeTs(normalizedEndTime)
        if (endTime < normalizedStartTime) {
            println("Failed to parse zoom-time argument:")
            println("  The given end time (${match.groupValues[2]}) exceeds the timestamp limits.")
            return null
        }
        return startTime..endTime
    }

    private fun plotMetric(
        metric: Metric,
        showPhases: Boolean,
        zoomTime: TimestampNsRange?,
        cliState: CliState
    ) {
        // Create output paths for data and scripts
        val metricOutputDirectory = cliState.output.pathForMetric(metric)
        val dataOutputDirectory = metricOutputDirectory.resolve(".data").also { it.toFile().mkdirs() }
        val rScriptDirectory = metricOutputDirectory.resolve(".R").also { it.toFile().mkdirs() }

        // Compute attributed resource usage for each leaf phase
        println("Computing resource attribution for metric \"${metric.path}\".")
        val phaseAttribution = cliState.resourceAttribution.leafPhases.mapNotNull { phase ->
            val attributionResult = cliState.resourceAttribution.attributeMetricToPhase(metric, phase)
            when {
                attributionResult is NoAttributedData -> null
                zoomTime != null -> phase to (attributionResult as AttributedResourceData).metricData
                    .slice(zoomTime.first, zoomTime.last)
                else -> phase to (attributionResult as AttributedResourceData).metricData
            }
        }.toMap()

        // Output list of leaf phases, if needed
        val selectedPhases = cliState.resourceAttribution.leafPhases
        val phaseListFile = dataOutputDirectory.resolve(PhaseListWriter.FILENAME).toFile()
        if (showPhases) {
            println("Writing list of leaf phases to \"${phaseListFile.absolutePath}\".")
            PhaseListWriter.output(
                phaseListFile,
                cliState.executionModel.rootPhase,
                selectedPhases,
                cliState.phaseList,
                cliState.time
            )
        }

        // Output list of leaf phase types, if needed
        val selectedPhaseTypes = selectedPhases.map { it.type }.toSet()
        val phaseTypeListFile = dataOutputDirectory.resolve(PhaseTypeListWriter.FILENAME).toFile()
        if (!showPhases) {
            println("Writing list of leaf phase types to \"${phaseTypeListFile.absolutePath}\".")
            PhaseTypeListWriter.output(phaseTypeListFile, selectedPhaseTypes, cliState)
        }

        // Output list of metrics
        val metricListFile = dataOutputDirectory.resolve(MetricListWriter.FILENAME).toFile()
        println("Writing list of metrics to \"${metricListFile.absolutePath}\".")
        MetricListWriter.output(metricListFile, listOf(metric), cliState.metricList)

        // Write raw metric data
        val metricDataFile = dataOutputDirectory.resolve(MetricDataWriter.FILENAME).toFile()
        println("Writing observed metric data to \"${metricDataFile.absolutePath}\".")
        MetricDataWriter.output(
            metricDataFile,
            listOf(metric),
            cliState,
            metricDataSelector = {
                if (zoomTime != null) it.data.slice(zoomTime.first, zoomTime.last)
                else it.data
            }
        )

        // Write upsampled metric data
        val upsampledMetricDataFile = dataOutputDirectory.resolve("upsampled-metric-data.tsv").toFile()
        println("Writing upsampled metric data to \"${upsampledMetricDataFile.absolutePath}\".")
        MetricDataWriter.output(
            upsampledMetricDataFile,
            listOf(metric),
            cliState,
            metricDataSelector = {
                if (zoomTime != null) {
                    cliState.resourceAttribution.upsampleMetric(it)!!
                        .slice(zoomTime.first, zoomTime.last)
                } else {
                    cliState.resourceAttribution.upsampleMetric(it)!!
                }
            }
        )

        // Write attributed resource usage per phase, if needed
        val resourceAttributionDataFile = dataOutputDirectory.resolve(ResourceAttributionDataWriter.FILENAME).toFile()
        if (showPhases) {
            println("Writing resource attribution data to \"${resourceAttributionDataFile.absolutePath}\".")
            ResourceAttributionDataWriter.output(
                resourceAttributionDataFile,
                mapOf(metric to phaseAttribution),
                cliState
            )
        }

        // Write attributed resource usage per phase type, if needed
        val aggregatedResourceAttributionDataFile = dataOutputDirectory
            .resolve(AggregateResourceAttributionDataWriter.FILENAME).toFile()
        if (!showPhases) {
            // Aggregate the attributed resource usage for all phases of a given type
            val phasesPerType = phaseAttribution.keys.groupBy { it.type }
            val aggregateResourceAttribution = phaseAttribution.entries
                .groupBy({ it.key.type }, { it.value })
                .mapValues { it.value.sum(it.value.first().maxValue) }
            // Filter out any phase type that do not occur during the given zoom period
            val selectedAggregateResourceAttribution = if (zoomTime != null) {
                val selectedTypes = phasesPerType.filter {
                    it.value.any { p -> p.startTime <= zoomTime.last && zoomTime.first <= p.endTime }
                }.keys
                aggregateResourceAttribution.filterKeys { it in selectedTypes }
            } else {
                aggregateResourceAttribution
            }
            println("Writing resource attribution data to \"${aggregatedResourceAttributionDataFile.absolutePath}\".")
            AggregateResourceAttributionDataWriter.output(
                aggregatedResourceAttributionDataFile,
                mapOf(metric to selectedAggregateResourceAttribution),
                phasesPerType,
                cliState
            )
        }

        // Instantiate the plot script template
        val rScriptFile = rScriptDirectory.resolve(SCRIPT_FILENAME).toFile()
        val plotOutputFile = metricOutputDirectory.resolve(PLOT_FILENAME).toFile()
        println("Instantiating R script to \"${rScriptFile.absolutePath}\".")
        instantiateRScript(
            rScriptFile, mapOf(
                "plot_filename" to "\"${plotOutputFile.name}\"",
                "output_directory" to metricOutputDirectory.toFile().asRPathString(),
                "data_directory" to dataOutputDirectory.toFile().asRPathString(),
                "metric_id" to "\"${cliState.metricList.metricToIdentifier(metric)}\"",
                "plot_per_phase" to if (showPhases) "TRUE" else "FALSE",
                "start_time" to if (zoomTime != null) "${cliState.time.normalize(zoomTime.first)}" else "-Inf",
                "end_time" to if (zoomTime != null) "${cliState.time.normalize(zoomTime.last)}" else "Inf"
            )
        )

        // Produce the plot using R
        println("Generating plot to \"${plotOutputFile.absolutePath}\".")
        val plotSuccessful = runRScript(rScriptFile)

        // Clean up if the plot succeeded
        if (plotSuccessful) {
            println("Cleaning up input files.")
            if (showPhases) phaseListFile.delete()
            else phaseTypeListFile.delete()
            metricListFile.delete()
            metricDataFile.delete()
            upsampledMetricDataFile.delete()
            if (showPhases) resourceAttributionDataFile.delete()
            else aggregatedResourceAttributionDataFile.delete()
        } else {
            println("Failed to generate plot, see \"${plotOutputFile.absoluteFile.nameWithoutExtension}.log\".")
        }
    }

}