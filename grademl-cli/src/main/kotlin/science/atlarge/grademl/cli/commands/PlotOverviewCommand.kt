package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.data.MetricDataWriter
import science.atlarge.grademl.cli.data.MetricListWriter
import science.atlarge.grademl.cli.data.PhaseListWriter
import science.atlarge.grademl.cli.util.ParsedCommand
import science.atlarge.grademl.cli.util.instantiateRScript
import science.atlarge.grademl.cli.util.runRScript
import science.atlarge.grademl.core.execution.ExecutionPhase
import science.atlarge.grademl.core.resources.Metric
import java.nio.file.Path

object PlotOverviewCommand : Command(
    name = "plot-overview",
    shortHelpMessage = "plot a comprehensive overview of the execution and resource model",
    longHelpMessage = "Plots an overview of the execution and resource model."
) {

    private const val PLOT_FILENAME = "overview.pdf"
    private const val SCRIPT_FILENAME = "plot-overview.R"

    override fun process(parsedCommand: ParsedCommand, cliState: CliState) {
        // Create output paths for data and scripts
        val outputDirectory = cliState.outputPath
        val dataOutputDirectory = outputDirectory.resolve(".data").also { it.toFile().mkdirs() }
        val rScriptDirectory = outputDirectory.resolve(".R").also { it.toFile().mkdirs() }

        // Select and output all phases in the execution model
        writePhaseList(
            cliState.executionModel.rootPhase,
            cliState.executionModel.phases,
            cliState,
            dataOutputDirectory
        )

        // Select and output all metrics in the resource model
        writeMetricList(
            cliState.resourceModel.resources.flatMap { it.metrics },
            cliState,
            dataOutputDirectory
        )

        // Write all metric data as time series to a file
        writeMetricData(
            cliState.resourceModel.resources.flatMap { it.metrics },
            cliState,
            dataOutputDirectory
        )

        // Instantiate the plot script template
        val rScriptFile = rScriptDirectory.resolve(SCRIPT_FILENAME).toFile()
        val plotOutputFile = dataOutputDirectory.resolve(PLOT_FILENAME).toFile()
        println("Instantiating R script to \"${rScriptFile.absolutePath}\".")
        instantiateRScript(rScriptFile, mapOf(
            "plot_filename" to plotOutputFile.name
        ))

        // Plot the overview using R
        println("Generating plot to \"${plotOutputFile.absolutePath}\".")
        runRScript(rScriptFile)
    }

    private fun writePhaseList(
        rootPhase: ExecutionPhase,
        selectedPhases: Iterable<ExecutionPhase>,
        cliState: CliState,
        dataOutputPath: Path
    ) {
        val outputFile = dataOutputPath.resolve(PhaseListWriter.FILENAME).toFile()
        println("Writing list of execution phases to \"${outputFile.absolutePath}\".")
        PhaseListWriter.output(outputFile, rootPhase, selectedPhases, cliState)
    }

    private fun writeMetricList(
        selectedMetrics: Iterable<Metric>,
        cliState: CliState,
        dataOutputPath: Path
    ) {
        val outputFile = dataOutputPath.resolve(MetricListWriter.FILENAME).toFile()
        println("Writing list of metrics to \"${outputFile.absolutePath}\".")
        MetricListWriter.output(outputFile, selectedMetrics, cliState)
    }

    private fun writeMetricData(
        selectedMetrics: Iterable<Metric>,
        cliState: CliState,
        dataOutputPath: Path
    ) {
        val outputFile = dataOutputPath.resolve(MetricDataWriter.FILENAME).toFile()
        println("Writing metric data to \"${outputFile.absolutePath}\".")
        MetricDataWriter.output(outputFile, selectedMetrics, cliState)
    }

}