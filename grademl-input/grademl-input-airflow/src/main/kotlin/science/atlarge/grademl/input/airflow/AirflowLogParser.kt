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

    private lateinit var runIds: Set<String>
    private lateinit var dagName: String
    private val taskNames = mutableSetOf<String>()
    private val taskDownstreamNames = mutableMapOf<String, MutableSet<String>>()
    private val taskStartTimesPerRun = mutableMapOf<String, MutableMap<String, TimestampNs>>()
    private val taskEndTimesPerRun = mutableMapOf<String, MutableMap<String, TimestampNs>>()

    // Expected directory structure:
    // <airflowLogDirectory>/
    //     run_id - identifier(s) for the DAG run(s) recorded in the given logs
    //     <dag_name>.dag - DAG structure, output of "airflow tasks list -t <dag_name>"
    //     <task_name>/ - subdirectory per task
    //         */*.log - log file for task run
    private fun parse(): AirflowLog {
        parseRunIds()
        findDagName()
        parseDagStructure()
        parseTaskInformation()
        return AirflowLog(runIds, dagName, taskNames, taskDownstreamNames, taskStartTimesPerRun, taskEndTimesPerRun)
    }

    private fun parseRunIds() {
        runIds = airflowLogDirectory.resolve("run_id")
            .toFile()
            .readLines()
            .filter { it.isNotEmpty() }
            .toSet()
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
        val taskLogs = airflowLogDirectory.toFile()
            .walk()
            .filter { it.isFile && it.extension == "log" }
            .map { it.parentFile.parentFile.name to it }
        // Parse the log files of every task
        for ((taskName, taskLogFile) in taskLogs) {
            val logLines = taskLogFile.readLines()

            // Find the run ID for this task
            val runId = logLines.first { it.startsWith("AIRFLOW_CTX_DAG_RUN_ID=") }
                .split("=", limit = 2)[1].trim()

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

            taskStartTimesPerRun.getOrPut(runId) { mutableMapOf() }[taskName] = startTime
            taskEndTimesPerRun.getOrPut(runId) { mutableMapOf() }[taskName] = endTime
        }
    }

    companion object {

        fun parseFromDirectory(airflowLogDirectory: Path): AirflowLog {
            return AirflowLogParser(airflowLogDirectory).parse()
        }

    }

}

data class AirflowLog(
    val runIds: Set<String>,
    val dagName: String,
    val taskNames: Set<String>,
    val taskDownstreamNames: Map<String, Set<String>>,
    val taskStartTimesPerRun: Map<String, Map<String, TimestampNs>>,
    val taskEndTimesPerRun: Map<String, Map<String, TimestampNs>>
)