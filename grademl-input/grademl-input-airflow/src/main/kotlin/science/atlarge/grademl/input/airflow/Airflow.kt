package science.atlarge.grademl.input.airflow

import science.atlarge.grademl.core.execution.ExecutionModel
import science.atlarge.grademl.core.execution.ExecutionPhase
import science.atlarge.grademl.core.input.InputSource
import science.atlarge.grademl.core.resources.ResourceModel
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.system.exitProcess

object Airflow : InputSource {

    override fun parseJobData(
        jobDataDirectories: Iterable<Path>,
        unifiedExecutionModel: ExecutionModel,
        unifiedResourceModel: ResourceModel
    ): Boolean {
        // Find Airflow log directories
        val airflowLogDirectories = jobDataDirectories
            .map { it.resolve("logs").resolve("airflow") }
            .filter { it.toFile().isDirectory }
        if (airflowLogDirectories.isEmpty()) return false

        // Parse Airflow log files
        val airflowLog = AirflowLogParser.parseFromDirectories(airflowLogDirectories)

        // Iterate over DAG runs to build the execution model
        for (runId in airflowLog.runIds) {
            // Determine which tasks exist for this run
            val tasksInRun = airflowLog.taskStartTimesPerRun[runId]?.keys.orEmpty()
            val dagStartTime = (airflowLog.taskStartTimesPerRun[runId]?.values?.minOrNull()
                ?: airflowLog.taskStartTimesPerRun.values.flatMap { it.values }.minOrNull() ?: 0L)
            val dagEndTime = (airflowLog.taskEndTimesPerRun[runId]?.values?.maxOrNull() ?: dagStartTime)
            // Add a phase for the DAG and for each task
            val dagPhase = unifiedExecutionModel.addPhase(
                name = airflowLog.dagName,
                tags = mapOf("run_id" to runId),
                startTime = dagStartTime,
                endTime = dagEndTime
            )
            val taskPhases = tasksInRun.associateWith { taskName ->
                unifiedExecutionModel.addPhase(
                    name = taskName,
                    startTime = airflowLog.taskStartTimesPerRun[runId]!![taskName]!!,
                    endTime = airflowLog.taskEndTimesPerRun[runId]!![taskName]!!,
                    parent = dagPhase
                )
            }
            // Add dataflow relationships between tasks
            for ((upstream, downstreams) in airflowLog.taskDownstreamNames) {
                val upstreamPhase = taskPhases[upstream] ?: continue
                for (downstream in downstreams) {
                    val downstreamPhase = taskPhases[downstream] ?: continue
                    upstreamPhase.addOutgoingDataflow(downstreamPhase)
                }
            }
        }

        return true
    }

}

// Wrapper for testing the log parser
fun main(args: Array<String>) {
    if (args.size != 1 || args[0] == "--help") {
        println("Arguments: <jobLogDirectory>")
        exitProcess(if (args.size != 1) -1 else 0)
    }

    val executionModel = ExecutionModel()
    val foundAirflowLogs = Airflow.parseJobData(listOf(Paths.get(args[0])), executionModel, ResourceModel())
    require(foundAirflowLogs) {
        "Cannot find Airflow logs in ${args[0]}"
    }
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
    for (topLevelPhase in executionModel.rootPhase.children.sortedBy { it.identifier }) {
        printPhase(topLevelPhase, "  ")
    }
}