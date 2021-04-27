package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.util.Option
import science.atlarge.grademl.cli.util.ParsedCommand
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
        println("Execution model extracted from job logs:")
        if (parsedCommand.isOptionProvided("short")) printShort(cliState.executionModel)
        else printLong(cliState.executionModel)
    }

    private fun printShort(executionModel: ExecutionModel) {
        fun printPhase(phase: ExecutionPhase, indent: String) {
            println("$indent/${phase.identifier}")
            for (childPhase in phase.children.sortedBy { it.identifier }) {
                printPhase(childPhase, "$indent  ")
            }
        }
        for (rootPhase in executionModel.rootPhases.sortedBy { it.identifier }) {
            printPhase(rootPhase, "  ")
        }
    }

    private fun printLong(executionModel: ExecutionModel) {
        fun printPhase(phase: ExecutionPhase, indent: String) {
            val outFlows = phase.outFlows.sortedBy { it.identifier }
            println("$indent/${phase.identifier}")
            println(
                "$indent      Start time:          %d.%09d"
                    .format(phase.startTime / 1_000_000_000, phase.startTime % 1_000_000_000)
            )
            println(
                "$indent      End time:            %d.%09d"
                    .format(phase.endTime / 1_000_000_000, phase.endTime % 1_000_000_000)
            )
            println("$indent      Outgoing dataflows:  (${outFlows.joinToString(", ") { it.identifier }})")
            for (childPhase in phase.children.sortedBy { it.identifier }) {
                printPhase(childPhase, "$indent  ")
            }
        }
        for (rootPhase in executionModel.rootPhases.sortedBy { it.identifier }) {
            printPhase(rootPhase, "  ")
        }
    }

}