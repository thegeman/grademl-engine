package science.atlarge.grademl.input.airflow.connections

import science.atlarge.grademl.core.execution.ExecutionModel
import science.atlarge.grademl.core.execution.ExecutionPhase
import science.atlarge.grademl.core.resources.ResourceModel
import science.atlarge.grademl.input.airflow.AirflowDagId
import science.atlarge.grademl.input.airflow.AirflowLog
import science.atlarge.grademl.input.airflow.AirflowRunId
import science.atlarge.grademl.input.airflow.AirflowTaskId

object AirflowSparkConnection : AirflowConnection {

    override fun processConnection(
        airflowLog: AirflowLog,
        phasesByDagRunAndTaskId: Map<AirflowDagId, Map<AirflowRunId, Map<AirflowTaskId, ExecutionPhase>>>,
        unifiedExecutionModel: ExecutionModel,
        unifiedResourceModel: ResourceModel
    ) {
        // Get a list of Spark application IDs to match
        val sparkAppIds = unifiedExecutionModel.rootPhase.children.filter { it.name == "SparkApplication" }
            .map { it.tags["id"]!! }
        if (sparkAppIds.isEmpty()) return
        // Go through all Airflow task runs and check for lines containing "Spark" and one or more Spark app IDs
        val matchedSparkApps = mutableMapOf<String, Triple<AirflowDagId, AirflowRunId, AirflowTaskId>>()
        for ((dagId, dagLog) in airflowLog.dagLogs) {
            for ((runId, taskLogs) in dagLog.taskLogContentsPerRunPerDag) {
                for ((taskId, taskLog) in taskLogs) {
                    // Find lines containing "Spark"
                    val sparkLines = taskLog.filter { "spark" in it.toLowerCase() }
                    // Find occurrences of any Spark application ID
                    val appIdsInTaskLog = sparkAppIds.filter { appId -> sparkLines.any { appId in it } }
                    // Make sure no application matches to more than one task run
                    require(matchedSparkApps.keys.intersect(appIdsInTaskLog).isEmpty()) {
                        "Found Spark application IDs referenced in more than one Airflow task's log"
                    }
                    for (appId in appIdsInTaskLog) {
                        matchedSparkApps[appId] = Triple(dagId, runId, taskId)
                    }
                }
            }
        }
        // Move Spark application phases to be children of the correct Airflow task phases
        // TODO: Implement option for changing parent of execution phases in ExecutionModel
    }

}