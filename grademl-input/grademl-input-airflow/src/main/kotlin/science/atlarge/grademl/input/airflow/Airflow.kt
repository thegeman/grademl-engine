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
            startTime = 0,
            endTime = 0
        )
        val taskPhases = airflowLog.taskNames.associateWith { taskName ->
            executionModel.addPhase(name = taskName, startTime = 0, endTime = 0)
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
        print("$indent/${phase.identifier}")
        if (outFlows.isNotEmpty()) {
            print("  ->  (${outFlows.joinToString(", ") { it.identifier }})")
        }
        println()
        for (childPhase in phase.children.sortedBy { it.identifier }) {
            printPhase(childPhase, "$indent  ")
        }
    }
    println("  Phases and dataflows:")
    for (rootPhase in executionModel.rootPhases.sortedBy { it.identifier }) {
        printPhase(rootPhase, "    ")
    }
}