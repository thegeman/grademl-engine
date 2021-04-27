package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.core.execution.ExecutionPhase

object DisplayExecutionModelCommand : Command {

    override val name: String
        get() = "display-execution-model"
    override val shortHelpMessage: String
        get() = "display detailed information about a job's execution model"
    override val longHelpMessage: String
        get() = ""

    override fun process(arguments: List<String>, cliState: CliState) {
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