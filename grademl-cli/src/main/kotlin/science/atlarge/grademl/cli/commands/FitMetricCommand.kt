package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.data.MetricDataWriter
import science.atlarge.grademl.cli.data.MetricListWriter
import science.atlarge.grademl.cli.data.PhaseListWriter
import science.atlarge.grademl.cli.terminal.Argument
import science.atlarge.grademl.cli.terminal.ArgumentValueConstraint
import science.atlarge.grademl.cli.terminal.ParsedCommand
import science.atlarge.grademl.cli.util.*
import science.atlarge.grademl.core.resources.Metric
import science.atlarge.grademl.core.util.asRPathString
import science.atlarge.grademl.core.util.instantiateRScript
import science.atlarge.grademl.core.util.runRScript

object FitMetricCommand : Command(
    name = "fit-metric",
    shortHelpMessage = "fit a linear relationship between phases and a metric",
    longHelpMessage = "Fits a linear relationship between execution phases and the observed utilization " +
            "of a resource, as measured by a particular metric.",
    supportedArguments = listOf(
        Argument(
            "metric_path",
            "path of metric to fit a relationship to",
            ArgumentValueConstraint.MetricPath,
            isOptional = false,
            isVararg = true
        )
    )
) {

    private const val SCRIPT_FILENAME = "fit-metric.R"

    override fun process(parsedCommand: ParsedCommand, cliState: CliState) {
        // Parse the metric_path argument value(s) for a set of metrics to fit
        val metrics = parsedCommand.getArgumentValues("metric_path")
            .map { parseMetricPathExpression(it) ?: return }
            .map { tryMatchMetricPath(it, cliState) ?: return }
            .flatten()
            .toSet()

        // Fit each metric independently
        var isFirst = true
        for (metric in metrics.sortedBy { cliState.metricList.metricToIdentifier(it) }) {
            if (!isFirst) println()
            isFirst = false
            println("Fitting linear model to metric: \"${metric.path}\".")
            fitMetric(metric, cliState)
        }
    }

    private fun fitMetric(metric: Metric, cliState: CliState) {
        // Create output paths for data and scripts
        val metricOutputDirectory = cliState.outputPathForMetric(metric)
        val dataOutputDirectory = metricOutputDirectory.resolve(".data").also { it.toFile().mkdirs() }
        val rScriptDirectory = metricOutputDirectory.resolve(".R").also { it.toFile().mkdirs() }

        // Output current metric's information
        val metricListFile = dataOutputDirectory.resolve(MetricListWriter.FILENAME).toFile()
        println("Writing current metric's metadata to \"${metricListFile.absolutePath}\".")
        MetricListWriter.output(
            metricListFile,
            listOf(metric),
            cliState
        )

        // Output selected phases in the execution model
        val phaseListFile = dataOutputDirectory.resolve(PhaseListWriter.FILENAME).toFile()
        println("Writing list of selected execution phases to \"${phaseListFile.absolutePath}\".")
        val selectedPhases = cliState.phaseFilter.includedPhases
        PhaseListWriter.output(
            phaseListFile,
            cliState.executionModel.rootPhase,
            selectedPhases,
            cliState
        )

        // Write metric data as time series to a file
        val metricDataFile = dataOutputDirectory.resolve(MetricDataWriter.FILENAME).toFile()
        println("Writing metric data to \"${metricDataFile.absolutePath}\".")
        MetricDataWriter.output(
            metricDataFile,
            listOf(metric),
            cliState,
            filterTime = cliState.executionModel.rootPhase.startTime..cliState.executionModel.rootPhase.endTime
        )

        // Instantiate the R script template
        val rScriptFile = rScriptDirectory.resolve(SCRIPT_FILENAME).toFile()
        //val plotOutputFile = phaseOutputDirectory.resolve(PLOT_FILENAME).toFile()
        println("Instantiating R script to \"${rScriptFile.absolutePath}\".")
        instantiateRScript(
            rScriptFile, mapOf(
                "output_directory" to metricOutputDirectory.toFile().asRPathString(),
                "data_directory" to dataOutputDirectory.toFile().asRPathString()
            )
        )

        // Plot the overview using R
        println("Fitting metric data to phase list.")
        runRScript(rScriptFile)
    }

}