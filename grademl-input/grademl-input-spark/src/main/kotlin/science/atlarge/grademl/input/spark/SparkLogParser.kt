package science.atlarge.grademl.input.spark

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import kotlin.streams.toList

class SparkLogParser private constructor(
    private val sparkLogDirectories: Iterable<Path>
) {

    private val appLogFiles = mutableSetOf<File>()

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
        // TODO: Parse more events from Spark logs
    }

    private fun parseAppId(groupedSparkEvents: Map<String, List<JsonObject>>): String {
        val applicationStartEvents = groupedSparkEvents["SparkListenerApplicationStart"]!!
        require(applicationStartEvents.size == 1) {
            "Found ${applicationStartEvents.size} SparkListenerApplicationStart events, expected 1"
        }
        return (applicationStartEvents[0]["App ID"] as JsonPrimitive).content
    }

    companion object {

        fun parseFromDirectories(sparkLogDirectories: Iterable<Path>): SparkLog {
            return SparkLogParser(sparkLogDirectories).parse()
        }

    }

}

class SparkLog()