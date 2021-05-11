package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.terminal.ArgumentValueConstraint
import science.atlarge.grademl.cli.terminal.Option
import science.atlarge.grademl.cli.terminal.OptionArgument
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
        Option('s', "short", "only display execution phase names"),
        Option(
            'd',
            "max-depth",
            "limit the depth of phases to display",
            argument = OptionArgument(
                "DEPTH",
                valueConstraint = ArgumentValueConstraint.Integer(0..Long.MAX_VALUE)
            )
        )
    )
) {

    override fun process(parsedCommand: ParsedCommand, cliState: CliState) {
        // Parse command options
        val verbose = !parsedCommand.isOptionProvided("short")
        val maxDepth = parseMaxDepth(parsedCommand) ?: return

        // Print (part of) the execution model
        printExecutionModel(cliState.executionModel, maxDepth, verbose)
    }

    private fun parseMaxDepth(parsedCommand: ParsedCommand): Int? {
        val maxDepthStr = parsedCommand.getOptionValue("max-depth") ?: return Int.MIN_VALUE
        val value = maxDepthStr.toLongOrNull()
        return when {
            value == null -> {
                println("Failed to parse depth value: \"$maxDepthStr\". Expected a non-negative integer value.")
                null
            }
            value < 0 -> {
                println("Maximum depth must be a non-negative integer.")
                null
            }
            value > Int.MAX_VALUE -> Int.MAX_VALUE
            else -> value.toInt()
        }
    }

    private fun printExecutionModel(executionModel: ExecutionModel, maxDepth: Int, verbose: Boolean) {
        println(
            "Execution model extracted from job logs${
                if (maxDepth >= 0) " (up to depth $maxDepth)" else ""
            }:"
        )
        printPhase(executionModel.rootPhase, 0, if (maxDepth >= 0) maxDepth else Int.MAX_VALUE, verbose)
    }

    private fun printPhase(phase: ExecutionPhase, depth: Int, maxDepth: Int, verbose: Boolean) {
        // Print the phase's identifier
        if (phase.isRoot) println("<root>")
        else println("${"  ".repeat(depth)}/${phase.identifier}")
        // Print more details if requested
        if (verbose) printPhaseDetails(phase, "  ".repeat(depth + 3))
        // Print recursively
        if (depth < maxDepth) {
            for (childPhase in phase.children.sortedBy { it.identifier }) {
                printPhase(childPhase, depth + 1, maxDepth, verbose)
            }
        }
    }

    private fun printPhaseDetails(phase: ExecutionPhase, indent: String) {
        val outFlows = phase.outFlows.sortedBy { it.identifier }

        println("${indent}Start time:          ${phase.startTime.toDisplayString()}")
        println("${indent}End time:            ${phase.endTime.toDisplayString()}")
        if (!phase.isRoot) {
            println("${indent}Outgoing dataflows:  (${outFlows.joinToString(", ") { it.identifier }})")
        }
    }

}