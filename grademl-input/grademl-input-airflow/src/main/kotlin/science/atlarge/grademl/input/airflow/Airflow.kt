package science.atlarge.grademl.input.airflow

import science.atlarge.grademl.core.execution.ExecutionModel
import science.atlarge.grademl.core.execution.ExecutionPhase
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.system.exitProcess

object Airflow {

    fun parseJobLogs(
        jobLogDirectory: Path,
        unifiedExecutionModel: ExecutionModel? = null
    ): ExecutionModel {
        // Parse Airflow logs
        val airflowLogDirectory = jobLogDirectory.resolve("logs").resolve("airflow-logs")
        require(airflowLogDirectory.toFile().isDirectory) { "Cannot find Airflow logs in $jobLogDirectory" }
        val airflowLog = AirflowLogParser.parseFromDirectory(airflowLogDirectory)

        // Add a phase for the DAG and for each task
        val executionModel = unifiedExecutionModel ?: ExecutionModel()
        val dagPhase = executionModel.addPhase(
            name = airflowLog.dagName,
            tags = mapOf("run_id" to airflowLog.runId),
            startTime = airflowLog.taskStartTimes.values.minOrNull() ?: 0L,
            endTime = airflowLog.taskEndTimes.values.maxOrNull() ?: 0L
        )
        val taskPhases = airflowLog.taskNames.associateWith { taskName ->
            executionModel.addPhase(
                name = taskName,
                startTime = airflowLog.taskStartTimes[taskName]!!,
                endTime = airflowLog.taskEndTimes[taskName]!!
            )
        }
        // Add parent-child relationships between DAG and tasks
        for (taskPhase in taskPhases.values) {
            dagPhase.addChild(taskPhase)
        }
        // Add dataflow relationships between tasks
        for ((upstream, downstreams) in airflowLog.taskDownstreamNames) {
            val upstreamPhase = taskPhases[upstream]!!
            for (downstream in downstreams) {
                val downstreamPhase = taskPhases[downstream]!!
                upstreamPhase.addOutgoingDataflow(downstreamPhase)
            }
        }

        return executionModel
    }

}

// Wrapper for testing the log parser
fun main(args: Array<String>) {
    if (args.size != 1 || args[0] == "--help") {
        println("Arguments: <jobLogDirectory>")
        exitProcess(if (args.size != 1) -1 else 0)
    }

    val executionModel = Airflow.parseJobLogs(Paths.get(args[0]))
    println("Execution model extracted from Airflow logs:")

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