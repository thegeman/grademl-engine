package science.atlarge.grademl.input.spark

import kotlinx.serialization.json.*
import science.atlarge.grademl.core.TimestampNs
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import kotlin.streams.toList

class SparkLogParser private constructor(
    private val sparkLogDirectories: Iterable<Path>
) {

    private val appLogFiles = mutableSetOf<File>()
    private val sparkJobsPerApp = mutableMapOf<String, List<SparkJobInfo>>()
    private val sparkStagesPerApp = mutableMapOf<String, List<SparkStageInfo>>()
    private val sparkTasksPerAppAndStage = mutableMapOf<String, Map<SparkStageId, List<SparkTaskInfo>>>()

    private fun parse(): SparkLog {
        findAppLogFiles()
        for (logFile in appLogFiles) {
            parseSparkLogFile(logFile)
        }
        return SparkLog()
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
        sparkJobsPerApp[appId] = parseSparkJobs(groupedSparkEvents)
        sparkStagesPerApp[appId] = parseSparkStages(groupedSparkEvents)
        sparkTasksPerAppAndStage[appId] = parseSparkTasks(groupedSparkEvents)
        // TODO: Parse more events from Spark logs
    }

    private fun parseAppId(groupedSparkEvents: Map<String, List<JsonObject>>): String {
        val applicationStartEvents = groupedSparkEvents["SparkListenerApplicationStart"]!!
        require(applicationStartEvents.size == 1) {
            "Found ${applicationStartEvents.size} SparkListenerApplicationStart events, expected 1"
        }
        return (applicationStartEvents[0]["App ID"] as JsonPrimitive).content
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
        require(startEventByJobId.keys.containsAll(endEventByJobId.keys) &&
                endEventByJobId.keys.containsAll(startEventByJobId.keys)) {
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

    private fun parseSparkStages(groupedSparkEvents: Map<String, List<JsonObject>>): List<SparkStageInfo> {
        // Find stage start and end events
        val startEventByStageId = groupedSparkEvents["SparkListenerStageSubmitted"].orEmpty()
            .map { it["Stage Info"] as JsonObject }
            .associateBy {
                SparkStageId(
                    (it["Stage ID"] as JsonPrimitive).content,
                    (it["Stage Attempt ID"] as JsonPrimitive).content
                )
            }
        val endEventByStageId = groupedSparkEvents["SparkListenerStageCompleted"].orEmpty()
            .map { it["Stage Info"] as JsonObject }
            .associateBy {
                SparkStageId(
                    (it["Stage ID"] as JsonPrimitive).content,
                    (it["Stage Attempt ID"] as JsonPrimitive).content
                )
            }
        // Make sure start and end events match up
        require(startEventByStageId.keys.containsAll(endEventByStageId.keys) &&
                endEventByStageId.keys.containsAll(startEventByStageId.keys)) {
            "Found mismatch between stage start and end events"
        }
        // Extract relevant stage information from start and end events
        return startEventByStageId.map { (stageId, startEvent) ->
            val endEvent = endEventByStageId[stageId]!!

            val startTime = (startEvent["Submission Time"] as JsonPrimitive).content.toLong() * 1_000_000
            val endTime = (endEvent["Completion Time"] as JsonPrimitive).content.toLong() * 1_000_000

            SparkStageInfo(stageId, startTime, endTime)
        }
    }

    private fun parseSparkTasks(
        groupedSparkEvents: Map<String, List<JsonObject>>
    ): Map<SparkStageId, List<SparkTaskInfo>> {
        // Find task start and end events
        val startEventsByStageIdAndTaskId = groupedSparkEvents["SparkListenerTaskStart"].orEmpty()
            .map { event ->
                val stageId = SparkStageId(
                    (event["Stage ID"] as JsonPrimitive).content,
                    (event["Stage Attempt ID"] as JsonPrimitive).content
                )
                val taskInfo = event["Task Info"] as JsonObject
                val taskId = SparkTaskId(
                    (taskInfo["Task ID"] as JsonPrimitive).content,
                    (taskInfo["Attempt"] as JsonPrimitive).content
                )
                stageId to (taskId to taskInfo)
            }
            .groupBy({ it.first }, { it.second })
            .mapValues { (_, eventsForStage) -> eventsForStage.toMap() }
        val endEventsByStageIdAndTaskId = groupedSparkEvents["SparkListenerTaskEnd"].orEmpty()
            .map { event ->
                val stageId = SparkStageId(
                    (event["Stage ID"] as JsonPrimitive).content,
                    (event["Stage Attempt ID"] as JsonPrimitive).content
                )
                val taskInfo = event["Task Info"] as JsonObject
                val taskId = SparkTaskId(
                    (taskInfo["Task ID"] as JsonPrimitive).content,
                    (taskInfo["Attempt"] as JsonPrimitive).content
                )
                stageId to (taskId to taskInfo)
            }
            .groupBy({ it.first }, { it.second })
            .mapValues { (_, eventsForStage) -> eventsForStage.toMap() }
        // Make sure start and end events match up
        require(startEventsByStageIdAndTaskId.keys.containsAll(endEventsByStageIdAndTaskId.keys) &&
                endEventsByStageIdAndTaskId.keys.containsAll(startEventsByStageIdAndTaskId.keys)) {
            "Found mismatch between task start and end events"
        }
        // Iterate over stages and interpret corresponding task events
        return startEventsByStageIdAndTaskId.mapValues { (stageId, startEvents) ->
            val endEvents = endEventsByStageIdAndTaskId[stageId]!!
            // Make sure start and end events match up
            require(startEvents.keys.containsAll(endEvents.keys) &&
                    endEvents.keys.containsAll(startEvents.keys)) {
                "Found mismatch between task start and end events"
            }
            // Extract relevant task information from start and end events
            startEvents.map { (taskId, startEvent) ->
                val endEvent = endEvents[taskId]!!

                val startTime = (startEvent["Launch Time"] as JsonPrimitive).content.toLong() * 1_000_000
                val endTime = (endEvent["Finish Time"] as JsonPrimitive).content.toLong() * 1_000_000

                SparkTaskInfo(taskId, startTime,  endTime)
            }
        }
    }

    companion object {

        fun parseFromDirectories(sparkLogDirectories: Iterable<Path>): SparkLog {
            return SparkLogParser(sparkLogDirectories).parse()
        }

    }

}

class SparkLog()

class SparkJobInfo(
    val id: String,
    val stages: List<String>,
    val startTime: TimestampNs,
    val endTime: TimestampNs
)

data class SparkStageId(val id: String, val attemptId: String)

class SparkStageInfo(
    val id: SparkStageId,
    val startTime: TimestampNs,
    val endTime: TimestampNs
)

data class SparkTaskId(val id: String, val attemptId: String)

class SparkTaskInfo(
    val id: SparkTaskId,
    val startTime: TimestampNs,
    val endTime: TimestampNs
)