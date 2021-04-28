package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.data.MetricDataWriter
import science.atlarge.grademl.cli.data.MetricListWriter
import science.atlarge.grademl.cli.data.PhaseListWriter
import science.atlarge.grademl.cli.util.ParsedCommand
import science.atlarge.grademl.cli.util.instantiateRScript
import science.atlarge.grademl.cli.util.runRScript
import science.atlarge.grademl.core.execution.ExecutionPhase

object PlotOverviewCommand : Command(
    name = "plot-overview",
    shortHelpMessage = "plot a comprehensive overview of the execution and resource model",
    longHelpMessage = "Plots an overview of the execution and resource model."
) {

    private const val PLOT_FILENAME = "overview.pdf"
    private const val SCRIPT_FILENAME = "plot-overview.R"

    override fun process(parsedCommand: ParsedCommand, cliState: CliState) {
        plotOverviewForPhase(cliState.executionModel.rootPhase, cliState)
    }

    private fun plotOverviewForPhase(phase: ExecutionPhase, cliState: CliState) {
        // Create output paths for data and scripts
        val phaseOutputDirectory = cliState.outputPathForPhase(phase)
        val dataOutputDirectory = phaseOutputDirectory.resolve(".data").also { it.toFile().mkdirs() }
        val rScriptDirectory = phaseOutputDirectory.resolve(".R").also { it.toFile().mkdirs() }

        // Output selected phases in the execution model
        val phaseListFile = dataOutputDirectory.resolve(PhaseListWriter.FILENAME).toFile()
        println("Writing list of selected execution phases to \"${phaseListFile.absolutePath}\".")
        PhaseListWriter.output(
            phaseListFile,
            cliState.executionModel.rootPhase,
            cliState.executionModel.phases,
            cliState
        )

        // Output selected metrics in the resource model
        val metricListFile = dataOutputDirectory.resolve(MetricListWriter.FILENAME).toFile()
        println("Writing list of selected metrics to \"${metricListFile.absolutePath}\".")
        MetricListWriter.output(
            metricListFile,
            cliState.selectedMetrics,
            cliState
        )

        // Write all metric data as time series to a file
        val metricDataFile = dataOutputDirectory.resolve(MetricDataWriter.FILENAME).toFile()
        println("Writing metric data to \"${metricDataFile.absolutePath}\".")
        MetricDataWriter.output(
            metricDataFile,
            cliState.selectedMetrics,
            cliState
        )

        // Instantiate the plot script template
        val rScriptFile = rScriptDirectory.resolve(SCRIPT_FILENAME).toFile()
        val plotOutputFile = phaseOutputDirectory.resolve(PLOT_FILENAME).toFile()
        println("Instantiating R script to \"${rScriptFile.absolutePath}\".")
        instantiateRScript(rScriptFile, mapOf("plot_filename" to "\"${plotOutputFile.name}\""))

        // Plot the overview using R
        println("Generating plot to \"${plotOutputFile.absolutePath}\".")
        runRScript(rScriptFile)
    }

}