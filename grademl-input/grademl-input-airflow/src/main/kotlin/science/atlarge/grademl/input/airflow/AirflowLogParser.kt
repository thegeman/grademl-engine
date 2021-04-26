package science.atlarge.grademl.input.airflow

import science.atlarge.grademl.core.TimestampNs
import java.nio.file.Path
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.*

class AirflowLogParser private constructor(
    private val airflowLogDirectory: Path
) {

    private lateinit var runId: String
    private lateinit var dagName: String
    private val taskNames = mutableSetOf<String>()
    private val taskStartTimes = mutableMapOf<String, TimestampNs>()
    private val taskEndTimes = mutableMapOf<String, TimestampNs>()
    private val taskDownstreamNames = mutableMapOf<String, MutableSet<String>>()

    // Expected directory structure:
    // <airflowLogDirectory>/
    //     run_id - identifier for the DAG run recorded in the given logs
    //     <dag_name>.dag - DAG structure, output of "airflow tasks list -t <dag_name>"
    //     <task_name>/ - subdirectory per task
    //         */*.log - log file for task run
    private fun parse(): AirflowLog {
        parseRunId()
        findDagName()
        parseDagStructure()
        parseTaskInformation()
        return AirflowLog(runId, dagName, taskNames, taskStartTimes, taskEndTimes, taskDownstreamNames)
    }

    private fun parseRunId() {
        runId = airflowLogDirectory.resolve("run_id")
            .toFile()
            .readLines()
            .first { it.isNotEmpty() }
            .trim()
    }

    private fun findDagName() {
        val dagFiles = airflowLogDirectory.toFile()
            .list { _, name -> name.endsWith(".dag") }
        require(dagFiles != null) { "No files matching '*.dag' found in $airflowLogDirectory" }
        require(dagFiles.size == 1) { "Too many files matching '*.dag' found in $airflowLogDirectory" }
        dagName = dagFiles.first().dropLast(".dag".length)
    }

    private fun parseDagStructure() {
        val dagStructureLines = airflowLogDirectory.resolve("$dagName.dag")
            .toFile()
            .readLines()
            .filter { it.isNotEmpty() }
        // Parse tree representation of the task DAG
        val taskNameStack = Stack<String>()
        val indentationStack = Stack<Int>()
        for (line in dagStructureLines) {
            // Get the task name and add it
            val taskName = line.split("): ").last().trimEnd('>')
            taskNames.add(taskName)
            // Get the indentation of the current line and pop from the stack any tasks
            // with the same or greater indentation, i.e., non-parent tasks
            val indentation = line.indexOf('<')
            while (indentationStack.isNotEmpty() && indentationStack.peek() >= indentation) {
                taskNameStack.pop()
                indentationStack.pop()
            }
            // Add downstream task constraint if applicable
            if (taskNameStack.isNotEmpty()) {
                val upstream = taskNameStack.peek()
                taskDownstreamNames.getOrPut(upstream) { mutableSetOf() }.add(taskName)
            }
            // Store task on indentation stack
            taskNameStack.push(taskName)
            indentationStack.push(indentation)
        }
    }

    private fun parseTaskInformation() {
        // Find and enumerate task log files
        val taskDirectories = airflowLogDirectory.toFile()
            .walkTopDown()
            .maxDepth(1)
            .filter { it.isDirectory }
        val taskLogsPerDirectory = taskDirectories
            .associate { dir ->
                dir.name to dir.walk().filter { it.isFile && it.extension == "log" }.first()
            }
        // Parse the log files of every task
        for ((taskName, taskLogFile) in taskLogsPerDirectory) {
            val logLines = taskLogFile.readLines()

            // Find records of the start and end time of the task
            val startTimeLine = logLines.first { "INFO - Executing <Task" in it }
            val endTimeLine = logLines.last { "INFO - Marking task" in it }

            // TODO: Log Airflow task start and end times in Unix format to avoid timezone issues
            // Parse dates and times (assuming local time)
            fun parseDateTime(dateTime: String): Long {
                val instant = LocalDateTime
                    .parse(dateTime, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS"))
                    .atZone(ZoneId.systemDefault())
                    .toInstant()
                return instant.epochSecond * 1_000_000_000L + instant.nano
            }

            val startTime = parseDateTime(startTimeLine.substring(1, startTimeLine.indexOf(']')))
            val endTime = parseDateTime(endTimeLine.substring(1, endTimeLine.indexOf(']')))

            taskStartTimes[taskName] = startTime
            taskEndTimes[taskName] = endTime
        }
    }

    companion object {

        fun parseFromDirectory(airflowLogDirectory: Path): AirflowLog {
            return AirflowLogParser(airflowLogDirectory).parse()
        }

    }

}

data class AirflowLog(
    val runId: String,
    val dagName: String,
    val taskNames: Set<String>,
    val taskStartTimes: Map<String, TimestampNs>,
    val taskEndTimes: Map<String, TimestampNs>,
    val taskDownstreamNames: Map<String, Set<String>>
)