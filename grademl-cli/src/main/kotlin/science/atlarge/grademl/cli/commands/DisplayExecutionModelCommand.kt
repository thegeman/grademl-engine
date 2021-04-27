package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.util.ParsedCommand
import science.atlarge.grademl.core.execution.ExecutionPhase

object DisplayExecutionModelCommand : Command(
    name = "display-execution-model",
    shortHelpMessage = "display detailed information about a job's execution model",
    longHelpMessage = "Displays information about a job's execution model. By default outputs a tree of execution phases,\n" +
            "and for each execution phase its start time, end time, and a list of outgoing dataflows."
) {

    override fun process(parsedCommand: ParsedCommand, cliState: CliState) {
        println("Execution model extracted from job logs:")

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
        for (rootPhase in cliState.executionModel.rootPhases.sortedBy { it.identifier }) {
            printPhase(rootPhase, "  ")
        }
    }

}