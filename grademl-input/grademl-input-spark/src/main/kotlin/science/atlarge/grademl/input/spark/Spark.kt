package science.atlarge.grademl.input.spark

import science.atlarge.grademl.core.input.InputSource
import science.atlarge.grademl.core.models.execution.ExecutionModel
import science.atlarge.grademl.core.models.execution.ExecutionPhase
import science.atlarge.grademl.core.models.resource.ResourceModel
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.system.exitProcess

object Spark : InputSource {

    override fun parseJobData(
        jobDataDirectories: Iterable<Path>,
        unifiedExecutionModel: ExecutionModel,
        unifiedResourceModel: ResourceModel
    ): Boolean {
        // Find Spark log directories
        val sparkLogDirectories = jobDataDirectories
            .map { it.resolve("logs").resolve("spark") }
            .filter { it.toFile().isDirectory }
        if (sparkLogDirectories.isEmpty()) return false

        // Parse Spark log files
        val sparkLog = SparkLogParser.parseFromDirectories(sparkLogDirectories)

        // Iterate over Spark applications to build the execution model
        for (appLog in sparkLog.sparkApps) {
            // Add execution phase for application
            val appPhase = unifiedExecutionModel.addPhase(
                name = "SparkApplication",
                tags = mapOf("id" to appLog.appInfo.id),
                startTime = appLog.appInfo.startTime,
                endTime = appLog.appInfo.endTime
            )
            // Add leaf phase for driver
            unifiedExecutionModel.addPhase(
                name = "Driver",
                startTime = appLog.appInfo.startTime,
                endTime = appLog.appInfo.endTime,
                parent = appPhase
            )
            // Add execution phases for stages
            val stageAttemptPhases = appLog.sparkAttemptsPerStage.flatMap { (_, attempts) ->
                attempts.map { stageAttempt ->
                    stageAttempt.attemptId to unifiedExecutionModel.addPhase(
                        name = "Stage",
                        tags = mapOf(
                            "id" to stageAttempt.attemptId.stageId.toString(),
                            "attempt" to stageAttempt.attemptId.attempt.toString()
                        ),
                        typeTags = setOf("id"),
                        startTime = stageAttempt.startTime,
                        endTime = stageAttempt.endTime,
                        parent = appPhase
                    )
                }
            }.toMap()
            // Add execution phases for tasks
            for ((stageAttemptId, stageAttempt) in appLog.sparkStageAttempts) {
                val stagePhase = stageAttemptPhases[stageAttemptId]!!
                for (taskAttemptId in stageAttempt.taskAttempts) {
                    val taskAttempt = appLog.sparkTaskAttempts[taskAttemptId]!!
                    unifiedExecutionModel.addPhase(
                        name = "Task",
                        tags = mapOf(
                            "id" to taskAttemptId.taskId.toString(),
                            "attempt" to taskAttemptId.attempt.toString()
                        ),
                        typeTags = emptySet(),
                        startTime = taskAttempt.startTime,
                        endTime = taskAttempt.endTime,
                        parent = stagePhase
                    )
                }
            }
            // Add dependencies between stages derived from job-level dependencies
            val attemptPhasesPerStage = stageAttemptPhases.entries
                .groupBy({ it.key.stageId }, { it.value })
            for ((jobId, jobDependencies) in appLog.sparkJobDependencies) {
                val targetPhases = appLog.sparkJobs[jobId]!!.stages.flatMap { attemptPhasesPerStage[it]!! }
                val sourcePhases = jobDependencies.flatMap { appLog.sparkJobs[it]!!.stages }
                    .flatMap { attemptPhasesPerStage[it]!! }
                for (source in sourcePhases) {
                    for (target in targetPhases) {
                        source.addOutgoingDataflow(target)
                    }
                }
            }
            // TODO: Add dependencies between stages within a job
            // TODO: Only add job-level dependencies between "source" and "sink" stages in a job
            // TODO: Consider how to deal with dependencies for stages with multiple attempts
        }

        return true
    }

}

// Wrapper for testing the log parser
fun main(args: Array<String>) {
    if (args.isEmpty() || args[0] == "--help") {
        println("Arguments: <jobLogDirectory> [...]")
        exitProcess(if (args.isEmpty()) -1 else 0)
    }

    val executionModel = ExecutionModel()
    val foundSparkLogs = Spark.parseJobData(args.map { Paths.get(it) }, executionModel, ResourceModel())
    require(foundSparkLogs) {
        "Cannot find Spark logs in any of the given jobLogDirectories"
    }
    println("Execution model extracted from Spark logs:")

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