package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.terminal.Option
import science.atlarge.grademl.cli.terminal.ParsedCommand
import science.atlarge.grademl.cli.util.toDisplayString
import science.atlarge.grademl.core.execution.ExecutionModel
import science.atlarge.grademl.core.execution.ExecutionPhase

object DisplayExecutionModelCommand : Command(
    name = "display-execution-model",
    shortHelpMessage = "display detailed information about a job's execution model",
    longHelpMessage = "Displays information about a job's execution model. By default outputs a tree of execution phases,\n" +
            "and for each execution phase its start time, end time, and a list of outgoing dataflows.",
    supportedOptions = listOf(
        Option('s', "short", "only display execution phase names")
    )
) {

    override fun process(parsedCommand: ParsedCommand, cliState: CliState) {
        printExecutionModel(
            cliState.executionModel,
            verbose = !parsedCommand.isOptionProvided("short")
        )
    }

    private fun printExecutionModel(executionModel: ExecutionModel, verbose: Boolean) {
        println("Execution model extracted from job logs:")
        for (topLevelPhase in executionModel.rootPhase.children.sortedBy { it.identifier }) {
            printPhase(topLevelPhase, "  ", verbose)
        }
    }

    private fun printPhase(phase: ExecutionPhase, indent: String, verbose: Boolean) {
        println("$indent/${phase.identifier}")
        if (verbose) printPhaseDetails(phase, "$indent      ")
        for (childPhase in phase.children.sortedBy { it.identifier }) {
            printPhase(childPhase, "$indent  ", verbose)
        }
    }

    private fun printPhaseDetails(phase: ExecutionPhase, indent: String) {
        val outFlows = phase.outFlows.sortedBy { it.identifier }

        println("${indent}Start time:          ${phase.startTime.toDisplayString()}")
        println("${indent}End time:            ${phase.endTime.toDisplayString()}")
        println("${indent}Outgoing dataflows:  (${outFlows.joinToString(", ") { it.identifier }})")
    }

}