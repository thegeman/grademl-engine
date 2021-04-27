package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.data.PhaseListWriter
import science.atlarge.grademl.cli.util.ParsedCommand
import science.atlarge.grademl.cli.util.PhaseList
import science.atlarge.grademl.core.execution.ExecutionPhase
import java.nio.file.Path

object PlotOverviewCommand : Command(
    name = "plot-overview",
    shortHelpMessage = "plot a comprehensive overview of the execution and resource model",
    longHelpMessage = "Plots an overview of the execution and resource model."
) {

    private const val SCRIPT_FILENAME = "plot-overview.R"

    override fun process(parsedCommand: ParsedCommand, cliState: CliState) {
        // Create output paths for data and scripts
        val outputPath = cliState.outputPath
        val dataOutputPath = outputPath.resolve(".data").also { it.toFile().mkdirs() }
        val rScriptPath = outputPath.resolve(".R").also { it.toFile().mkdirs() }

        // Select and output all phases in the execution model
        writePhaseList(
            cliState.executionModel.rootPhase,
            cliState.executionModel.phases,
            cliState,
            dataOutputPath
        )

        // Select and output all metrics in the resource model
        TODO()

        // Plot the overview using R
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

}