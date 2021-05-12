package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.terminal.*
import science.atlarge.grademl.cli.util.toDisplayString
import science.atlarge.grademl.cli.util.tryMatchExecutionPhasePath
import science.atlarge.grademl.core.execution.ExecutionPhase
import science.atlarge.grademl.core.execution.ExecutionPhasePath

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
    ),
    supportedArguments = listOf(
        Argument(
            "phase_path",
            "phases to display, along with their descendants (default: /)",
            valueConstraint = ArgumentValueConstraint.ExecutionPhasePath,
            isOptional = true,
            isVararg = true
        )
    )
) {

    override fun process(parsedCommand: ParsedCommand, cliState: CliState) {
        // Parse command options
        val verbose = !parsedCommand.isOptionProvided("short")
        val maxDepth = parseMaxDepth(parsedCommand) ?: return
        val givenPhases = parsePhasePaths(parsedCommand, cliState) ?: return
        val selectedPhases = givenPhases.ifEmpty { listOf(cliState.executionModel.rootPhase) }

        // For each given phase, print (part of) the execution model rooted in that phase
        selectedPhases.forEachIndexed { i, phase ->
            if (i > 0) println()
            printExecutionModel(phase, cliState, maxDepth, verbose)
        }
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

    private fun parsePhasePaths(parsedCommand: ParsedCommand, cliState: CliState): List<ExecutionPhase>? {
        val phasePathExpressions = parsedCommand.getArgumentValues("phase_path")
        if (phasePathExpressions.isEmpty()) return emptyList()
        // Try to match each phase path expression
        return phasePathExpressions.flatMap { phasePathExpression ->
            tryMatchExecutionPhasePath(ExecutionPhasePath.parse(phasePathExpression), cliState) ?: return null
        }
    }

    private fun printExecutionModel(rootPhase: ExecutionPhase, cliState: CliState, maxDepth: Int, verbose: Boolean) {
        println(
            "Displaying execution model rooted in the following phase${
                if (maxDepth >= 0) " (up to depth $maxDepth)" else ""
            }:"
        )
        printPhase(rootPhase, cliState, 0, if (maxDepth >= 0) maxDepth else Int.MAX_VALUE, verbose)
    }

    private fun printPhase(phase: ExecutionPhase, cliState: CliState, depth: Int, maxDepth: Int, verbose: Boolean) {
        // Skip this phase if has been excluded in the CLI
        if (!phase.isRoot && phase !in cliState.phaseFilter.includedPhases) return
        // Print the phase's identifier (or path, for the root phase of this subtree)
        when {
            phase.isRoot -> println("<root>")
            depth == 0 -> println("${phase.path}")
            else -> println("${"  ".repeat(depth)}/${phase.identifier}")
        }
        // Print more details if requested
        if (verbose) printPhaseDetails(phase, cliState, "  ".repeat(depth + 3))
        // Print recursively
        if (depth < maxDepth) {
            // Skip child phases that have been excluded in the CLI
            val selectedChildren = phase.children.intersect(cliState.phaseFilter.includedPhases)
            for (childPhase in selectedChildren.sortedBy { it.identifier }) {
                printPhase(childPhase, cliState, depth + 1, maxDepth, verbose)
            }
        }
    }

    private fun printPhaseDetails(phase: ExecutionPhase, cliState: CliState, indent: String) {
        val startTime = cliState.time.normalize(phase.startTime).toDisplayString()
        val endTime = cliState.time.normalize(phase.endTime).toDisplayString()
        val outFlows = phase.outFlows.sortedBy { it.identifier }

        println("${indent}Start time:          ${startTime.padStart(endTime.length, ' ')}")
        println("${indent}End time:            $endTime")
        if (!phase.isRoot) {
            println("${indent}Outgoing dataflows:  (${outFlows.joinToString(", ") { it.identifier }})")
        }
    }

}