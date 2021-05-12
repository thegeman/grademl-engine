package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.data.MetricDataWriter
import science.atlarge.grademl.cli.data.MetricListWriter
import science.atlarge.grademl.cli.data.PhaseListWriter
import science.atlarge.grademl.cli.terminal.Argument
import science.atlarge.grademl.cli.terminal.ArgumentValueConstraint
import science.atlarge.grademl.cli.terminal.ParsedCommand
import science.atlarge.grademl.cli.util.*
import science.atlarge.grademl.core.execution.ExecutionPhase
import science.atlarge.grademl.core.util.asRPathString
import science.atlarge.grademl.core.util.instantiateRScript
import science.atlarge.grademl.core.util.runRScript

object PlotOverviewCommand : Command(
    name = "plot-overview",
    shortHelpMessage = "plot a comprehensive overview of the execution and resource model",
    longHelpMessage = "Plots an overview of the execution and resource model.",
    supportedArguments = listOf(
        Argument(
            "phase_path",
            "path of phase to plot an overview for (defaults to root phase)",
            ArgumentValueConstraint.ExecutionPhasePath,
            isOptional = true,
            isVararg = true
        )
    )
) {

    private const val PLOT_FILENAME = "overview.pdf"
    private const val SCRIPT_FILENAME = "plot-overview.R"

    override fun process(parsedCommand: ParsedCommand, cliState: CliState) {
        val phaseExpressions = parsedCommand.getArgumentValues("phase_path").ifEmpty { listOf("/") }
        val phasePaths = phaseExpressions.map(::parseExecutionPhasePathExpression)
        val phases = phasePaths.flatMap { path -> tryMatchExecutionPhasePath(path, cliState) ?: return }.toSet()

        var isFirst = true
        for (phase in phases.sortedBy { cliState.phaseList.phaseToIdentifier(it) }) {
            if (!isFirst) println()
            isFirst = false
            println("Plotting overview for phase: \"${phase.path}\".")
            plotOverviewForPhase(phase, cliState)
        }
    }

    private fun plotOverviewForPhase(phase: ExecutionPhase, cliState: CliState) {
        // Create output paths for data and scripts
        val phaseOutputDirectory = cliState.output.pathForPhase(phase)
        val dataOutputDirectory = phaseOutputDirectory.resolve(".data").also { it.toFile().mkdirs() }
        val rScriptDirectory = phaseOutputDirectory.resolve(".R").also { it.toFile().mkdirs() }

        // Output selected phases in the execution model
        val phaseListFile = dataOutputDirectory.resolve(PhaseListWriter.FILENAME).toFile()
        println("Writing list of selected execution phases to \"${phaseListFile.absolutePath}\".")
        val selectedPhases = phase.descendants
        PhaseListWriter.output(
            phaseListFile,
            phase,
            selectedPhases,
            cliState.phaseList,
            cliState.time
        )

        // Output selected metrics in the resource model
        val metricListFile = dataOutputDirectory.resolve(MetricListWriter.FILENAME).toFile()
        println("Writing list of selected metrics to \"${metricListFile.absolutePath}\".")
        MetricListWriter.output(
            metricListFile,
            cliState.metricFilter.includedMetrics,
            cliState.metricList
        )

        // Write all metric data as time series to a file
        val metricDataFile = dataOutputDirectory.resolve(MetricDataWriter.FILENAME).toFile()
        println("Writing metric data to \"${metricDataFile.absolutePath}\".")
        MetricDataWriter.output(
            metricDataFile,
            cliState.metricFilter.includedMetrics,
            cliState,
            filterTime = selectedPhases.minOf { it.startTime }..selectedPhases.maxOf { it.endTime }
        )

        // Instantiate the plot script template
        val rScriptFile = rScriptDirectory.resolve(SCRIPT_FILENAME).toFile()
        val plotOutputFile = phaseOutputDirectory.resolve(PLOT_FILENAME).toFile()
        println("Instantiating R script to \"${rScriptFile.absolutePath}\".")
        instantiateRScript(
            rScriptFile, mapOf(
                "plot_filename" to "\"${plotOutputFile.name}\"",
                "output_directory" to phaseOutputDirectory.toFile().asRPathString(),
                "data_directory" to dataOutputDirectory.toFile().asRPathString()
            )
        )

        // Plot the overview using R
        println("Generating plot to \"${plotOutputFile.absolutePath}\".")
        val plotSuccessful = runRScript(rScriptFile)

        // Clean up if the plot succeeded
        if (plotSuccessful) {
            println("Cleaning up input files.")
            phaseListFile.delete()
            metricListFile.delete()
            metricDataFile.delete()
        } else {
            println("Failed to generate plot, see \"${plotOutputFile.absoluteFile.nameWithoutExtension}.log\".")
        }
    }

}