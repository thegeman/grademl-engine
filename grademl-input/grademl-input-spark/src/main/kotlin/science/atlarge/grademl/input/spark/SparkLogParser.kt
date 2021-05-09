package science.atlarge.grademl.input.spark

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import science.atlarge.grademl.core.TimestampNs
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import kotlin.streams.toList

typealias SparkAppId = String
typealias SparkJobId = String
typealias SparkStageId = String
typealias SparkTaskId = String

data class SparkStageAttemptId(val id: SparkStageId, val attempt: String)
data class SparkTaskAttemptId(val id: SparkTaskId, val attempt: String)

class SparkLogParser private constructor(
    private val sparkLogDirectories: Iterable<Path>
) {

    private val appLogFiles = mutableSetOf<File>()
    private val sparkApps = mutableListOf<SparkAppInfo>()
    private val sparkJobsPerApp = mutableMapOf<SparkAppId, List<SparkJobInfo>>()
    private val sparkStagesPerApp = mutableMapOf<SparkAppId, List<SparkStageInfo>>()
    private val sparkTasksPerApp = mutableMapOf<SparkAppId, List<SparkTaskInfo>>()
    private val sparkJobDependenciesPerApp = mutableMapOf<SparkAppId, Map<SparkJobId, Set<SparkJobId>>>()

    private fun parse(): SparkLog {
        findAppLogFiles()
        for (logFile in appLogFiles) {
            parseSparkLogFile(logFile)
        }
        return SparkLog(
            sparkApps,
            sparkJobsPerApp,
            sparkStagesPerApp,
            sparkTasksPerApp,
            sparkJobDependenciesPerApp
        )
    }

    private fun findAppLogFiles() {
        // Find all files in the Spark log directory
        val logFiles = sparkLogDirectories.flatMap { directory ->
            Files.list(directory).use { fileList ->
                fileList.map { it.toFile() }
                    .filter { it.isFile }
                    .toList()
            }
        }
        // Check each file for a Spark application ID
        for (logFile in logFiles) {
            // Find the first ApplicationStart event, if it exists in this file, to verify that this file is
            // a Spark application log file
            val applicationStartLine = logFile.useLines { lines ->
                lines.firstOrNull { "SparkListenerApplicationStart" in it }
            } ?: continue
            try {
                val eventJson = Json.parseToJsonElement(applicationStartLine)
                if (eventJson !is JsonObject) continue
                val appIdJson = eventJson["App ID"] ?: continue
                if (appIdJson !is JsonPrimitive) continue

                appLogFiles.add(logFile)
            } catch (_: Exception) {
                continue
            }
        }
    }

    private fun parseSparkLogFile(logFile: File) {
        // Parse each line in the file to a JSON object representing one Spark event
        val sparkEvents = logFile.useLines { lines ->
            lines.filter { it.isNotBlank() }
                .map { Json.parseToJsonElement(it) as JsonObject }
                .toList()
        }
        // Group events by event type for easier look-ups
        val groupedSparkEvents = sparkEvents.groupBy { (it["Event"] as JsonPrimitive).content }
        // Parse different kinds of events for relevant information
        val appId = parseAppId(groupedSparkEvents)
        sparkApps.add(parseAppInfo(groupedSparkEvents))
        sparkJobsPerApp[appId] = parseSparkJobs(groupedSparkEvents)
        val (tasks, stagesToTasksMap) = parseSparkTasks(groupedSparkEvents)
        sparkTasksPerApp[appId] = tasks
        sparkStagesPerApp[appId] = parseSparkStages(groupedSparkEvents, stagesToTasksMap)
        sparkJobDependenciesPerApp[appId] = inferJobDependencies(sparkJobsPerApp[appId]!!)
        // TODO: Parse RDD info if useful
        // TODO: Parse stage-level dependencies
    }

    private fun parseAppId(groupedSparkEvents: Map<String, List<JsonObject>>): String {
        val applicationStartEvents = groupedSparkEvents["SparkListenerApplicationStart"]!!
        require(applicationStartEvents.size == 1) {
            "Found ${applicationStartEvents.size} SparkListenerApplicationStart events, expected 1"
        }
        return (applicationStartEvents[0]["App ID"] as JsonPrimitive).content
    }

    private fun parseAppInfo(groupedSparkEvents: Map<String, List<JsonObject>>): SparkAppInfo {
        val startEvent = groupedSparkEvents["SparkListenerApplicationStart"]!![0]
        val endEvent = groupedSparkEvents["SparkListenerApplicationEnd"]!!.let { events ->
            require(events.size == 1) {
                "Found ${events.size} SparkListenerApplicationEnd events, expected 1"
            }
            events[0]
        }
        // Extract the start and end time of the application from the events
        val appId = (startEvent["App ID"] as JsonPrimitive).content
        val startTime = (startEvent["Timestamp"] as JsonPrimitive).content.toLong() * 1_000_000
        val endTime = (endEvent["Timestamp"] as JsonPrimitive).content.toLong() * 1_000_000
        return SparkAppInfo(appId, startTime, endTime)
    }

    private fun parseSparkJobs(groupedSparkEvents: Map<String, List<JsonObject>>): List<SparkJobInfo> {
        // Find job start and end events
        val startEventByJobId = groupedSparkEvents["SparkListenerJobStart"].orEmpty().associateBy {
            (it["Job ID"] as JsonPrimitive).content
        }
        val endEventByJobId = groupedSparkEvents["SparkListenerJobEnd"].orEmpty().associateBy {
            (it["Job ID"] as JsonPrimitive).content
        }
        // Make sure start and end events match up
        require(
            startEventByJobId.keys.containsAll(endEventByJobId.keys) &&
                    endEventByJobId.keys.containsAll(startEventByJobId.keys)
        ) {
            "Found mismatch between job start and end events"
        }
        // Extract relevant job information from start and end events
        return startEventByJobId.map { (jobId, startEvent) ->
            val endEvent = endEventByJobId[jobId]!!

            val stages = (startEvent["Stage IDs"] as JsonArray).map { (it as JsonPrimitive).content }
            val startTime = (startEvent["Submission Time"] as JsonPrimitive).content.toLong() * 1_000_000
            val endTime = (endEvent["Completion Time"] as JsonPrimitive).content.toLong() * 1_000_000

            SparkJobInfo(jobId, stages, startTime, endTime)
        }
    }

    private fun parseSparkStages(
        groupedSparkEvents: Map<String, List<JsonObject>>,
        stagesToTasksMap: Map<SparkStageAttemptId, List<SparkTaskAttemptId>>
    ): List<SparkStageInfo> {
        // Find stage start and end events
        val startEventByStageId = groupedSparkEvents["SparkListenerStageSubmitted"].orEmpty()
            .map { it["Stage Info"] as JsonObject }
            .associateBy {
                SparkStageAttemptId(
                    (it["Stage ID"] as JsonPrimitive).content,
                    (it["Stage Attempt ID"] as JsonPrimitive).content
                )
            }
        val endEventByStageId = groupedSparkEvents["SparkListenerStageCompleted"].orEmpty()
            .map { it["Stage Info"] as JsonObject }
            .associateBy {
                SparkStageAttemptId(
                    (it["Stage ID"] as JsonPrimitive).content,
                    (it["Stage Attempt ID"] as JsonPrimitive).content
                )
            }
        // Make sure start and end events match up
        require(
            startEventByStageId.keys.containsAll(endEventByStageId.keys) &&
                    endEventByStageId.keys.containsAll(startEventByStageId.keys)
        ) {
            "Found mismatch between stage start and end events"
        }
        // Extract relevant stage information from start and end events
        return startEventByStageId.map { (stageId, startEvent) ->
            val endEvent = endEventByStageId[stageId]!!

            val startTime = (startEvent["Submission Time"] as JsonPrimitive).content.toLong() * 1_000_000
            val endTime = (endEvent["Completion Time"] as JsonPrimitive).content.toLong() * 1_000_000
            val taskIds = stagesToTasksMap[stageId].orEmpty()

            SparkStageInfo(stageId, taskIds, startTime, endTime)
        }
    }

    private fun parseSparkTasks(
        groupedSparkEvents: Map<String, List<JsonObject>>
    ): Pair<List<SparkTaskInfo>, Map<SparkStageAttemptId, List<SparkTaskAttemptId>>> {
        // While parsing task information, create a mapping from stage attempts to task attempts
        val stageToTaskMap = mutableMapOf<SparkStageAttemptId, MutableList<SparkTaskAttemptId>>()
        // Find task start and end events
        val startEventsByTaskId = groupedSparkEvents["SparkListenerTaskStart"].orEmpty().associate { event ->
            val stageId = SparkStageAttemptId(
                (event["Stage ID"] as JsonPrimitive).content,
                (event["Stage Attempt ID"] as JsonPrimitive).content
            )
            val taskInfo = event["Task Info"] as JsonObject
            val taskId = SparkTaskAttemptId(
                (taskInfo["Task ID"] as JsonPrimitive).content,
                (taskInfo["Attempt"] as JsonPrimitive).content
            )
            stageToTaskMap.getOrPut(stageId) { mutableListOf() }.add(taskId)
            taskId to taskInfo
        }
        val endEventsByTaskId = groupedSparkEvents["SparkListenerTaskEnd"].orEmpty().associate { event ->
            val taskInfo = event["Task Info"] as JsonObject
            val taskId = SparkTaskAttemptId(
                (taskInfo["Task ID"] as JsonPrimitive).content,
                (taskInfo["Attempt"] as JsonPrimitive).content
            )
            taskId to taskInfo
        }
        // Make sure start and end events match up
        require(
            startEventsByTaskId.keys.containsAll(endEventsByTaskId.keys) &&
                    endEventsByTaskId.keys.containsAll(startEventsByTaskId.keys)
        ) {
            "Found mismatch between task start and end events"
        }
        // Extract relevant task information from start and end events
        return startEventsByTaskId.map { (taskId, startEvent) ->
            val endEvent = endEventsByTaskId[taskId]!!

            val startTime = (startEvent["Launch Time"] as JsonPrimitive).content.toLong() * 1_000_000
            val endTime = (endEvent["Finish Time"] as JsonPrimitive).content.toLong() * 1_000_000

            SparkTaskInfo(taskId, startTime, endTime)
        } to stageToTaskMap
    }

    private fun inferJobDependencies(jobs: List<SparkJobInfo>): Map<String, Set<String>> {
        // Infer job dependencies from start and end times of jobs,
        // assuming that a job depends on all jobs that ended before its start time
        val jobDependencies = mutableMapOf<String, Set<String>>()
        val completedJobs = mutableSetOf<String>()
        val jobQueue = jobs.flatMap { job ->
            listOf(
                (job.startTime to 1) to job.id,
                (job.endTime to -1) to job.id
            )
        }.sortedWith(compareBy({ it.first.first }, { it.first.second }))
        for (jobChange in jobQueue) {
            when (jobChange.first.second) {
                // Process the start of a job
                1 -> {
                    if (completedJobs.isNotEmpty()) {
                        jobDependencies[jobChange.second] = completedJobs.toSet()
                    }
                }
                // Process the end of a job
                -1 -> {
                    completedJobs.removeAll(jobDependencies[jobChange.second].orEmpty())
                    completedJobs.add(jobChange.second)
                }
            }
        }
        return jobDependencies
    }

    companion object {

        fun parseFromDirectories(sparkLogDirectories: Iterable<Path>): SparkLog {
            return SparkLogParser(sparkLogDirectories).parse()
        }

    }

}

class SparkLog(
    val appIds: List<SparkAppInfo>,
    val sparkJobsPerApp: Map<SparkAppId, List<SparkJobInfo>>,
    val sparkStagesPerApp: Map<SparkAppId, List<SparkStageInfo>>,
    val sparkTasksPerApp: Map<SparkAppId, List<SparkTaskInfo>>,
    val sparkJobDependenciesPerApp: Map<SparkAppId, Map<SparkJobId, Set<SparkJobId>>>
)

class SparkAppInfo(
    val id: SparkAppId,
    val startTime: TimestampNs,
    val endTime: TimestampNs
)

class SparkJobInfo(
    val id: SparkJobId,
    val stages: List<SparkStageId>,
    val startTime: TimestampNs,
    val endTime: TimestampNs
)

class SparkStageInfo(
    val attemptId: SparkStageAttemptId,
    val tasks: List<SparkTaskAttemptId>,
    val startTime: TimestampNs,
    val endTime: TimestampNs
)

class SparkTaskInfo(
    val attemptId: SparkTaskAttemptId,
    val startTime: TimestampNs,
    val endTime: TimestampNs
)