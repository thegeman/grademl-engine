package science.atlarge.grademl.input.airflow

import science.atlarge.grademl.core.execution.ExecutionModel
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
            tags = mapOf("run_id" to airflowLog.runId)
        )
        val taskPhases = airflowLog.taskNames.associateWith { taskName ->
            executionModel.addPhase(name = taskName)
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