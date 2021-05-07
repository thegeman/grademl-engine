package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.data.MetricDataWriter
import science.atlarge.grademl.cli.data.PhaseListWriter
import science.atlarge.grademl.cli.data.ResourceAttributionDataWriter
import science.atlarge.grademl.cli.terminal.Argument
import science.atlarge.grademl.cli.terminal.ArgumentValueConstraint
import science.atlarge.grademl.cli.terminal.ParsedCommand
import science.atlarge.grademl.cli.util.parseMetricPathExpression
import science.atlarge.grademl.cli.util.tryMatchMetricPath
import science.atlarge.grademl.core.attribution.*
import science.atlarge.grademl.core.resources.Metric
import science.atlarge.grademl.core.util.asRPathString
import science.atlarge.grademl.core.util.instantiateRScript
import science.atlarge.grademl.core.util.runRScript

object PlotMetricCommand : Command(
    name = "plot-metric",
    shortHelpMessage = "plot a comprehensive view of a particular metric",
    longHelpMessage = "Plots a metric's observed time series, its upsampled time series, " +
            "and its attribution to phases.",
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
        // Parse the metric_path argument value(s) for a set of metrics to plot
        val metrics = parsedCommand.getArgumentValues("metric_path")
            .map { parseMetricPathExpression(it) ?: return }
            .map { tryMatchMetricPath(it, cliState) ?: return }
            .flatten()
            .toSet()

        // Configure resource attribution process
        val resourceAttribution = ResourceAttribution(
            cliState.executionModel,
            cliState.resourceModel,
            BestFitAttributionRuleProvider.from(cliState.executionModel, cliState.resourceModel, cliState.outputPath)
        )

        // Plot each metric
        for (metric in metrics) {
            plotMetric(metric, resourceAttribution, cliState)
        }
    }

    private fun plotMetric(
        metric: Metric,
        resourceAttribution: ResourceAttribution,
        cliState: CliState
    ) {
        // Create output paths for data and scripts
        val metricOutputDirectory = cliState.outputPathForMetric(metric)
        val dataOutputDirectory = metricOutputDirectory.resolve(".data").also { it.toFile().mkdirs() }
        val rScriptDirectory = metricOutputDirectory.resolve(".R").also { it.toFile().mkdirs() }

        // Compute attributed resource usage for each leaf phase
        println("Computing resource attribution for metric \"${metric.path}\".")
        val phaseAttribution = resourceAttribution.leafPhases.associateWith {
            resourceAttribution.attributeMetricToPhase(metric, it)!!
        }

        // Output list of leaf phases
        val phaseListFile = dataOutputDirectory.resolve(PhaseListWriter.FILENAME).toFile()
        println("Writing list of leaf phases to \"${phaseListFile.absolutePath}\".")
        val selectedPhases = resourceAttribution.leafPhases
        PhaseListWriter.output(
            phaseListFile,
            cliState.executionModel.rootPhase,
            selectedPhases,
            cliState
        )

        // Write raw metric data
        val metricDataFile = dataOutputDirectory.resolve(MetricDataWriter.FILENAME).toFile()
        println("Writing observed metric data to \"${metricDataFile.absolutePath}\".")
        MetricDataWriter.output(
            metricDataFile,
            listOf(metric),
            cliState
        )

        // Write upsampled metric data
        val upsampledMetricDataFile = dataOutputDirectory.resolve("upsampled-metric-data.tsv").toFile()
        println("Writing upsampled metric data to \"${upsampledMetricDataFile.absolutePath}\".")
        MetricDataWriter.output(
            upsampledMetricDataFile,
            listOf(metric),
            cliState,
            metricDataSelector = { resourceAttribution.upsampleMetric(it)!! }
        )

        // Write attributed resource usage
        val resourceAttributionDataFile = dataOutputDirectory.resolve(ResourceAttributionDataWriter.FILENAME).toFile()
        println("Writing resource attribution data to \"${resourceAttributionDataFile.absolutePath}\".")
        ResourceAttributionDataWriter.output(
            resourceAttributionDataFile,
            mapOf(metric to phaseAttribution),
            cliState
        )

        // Instantiate the plot script template
        val rScriptFile = rScriptDirectory.resolve(SCRIPT_FILENAME).toFile()
        val plotOutputFile = metricOutputDirectory.resolve(PLOT_FILENAME).toFile()
        println("Instantiating R script to \"${rScriptFile.absolutePath}\".")
        instantiateRScript(
            rScriptFile, mapOf(
                "plot_filename" to "\"${plotOutputFile.name}\"",
                "output_directory" to metricOutputDirectory.toFile().asRPathString(),
                "data_directory" to dataOutputDirectory.toFile().asRPathString(),
                "metric_id" to "\"${cliState.metricList.metricToIdentifier(metric)}\""
            )
        )

        // Produce the plot using R
        println("Generating plot to \"${plotOutputFile.absolutePath}\".")
        runRScript(rScriptFile)
    }

}