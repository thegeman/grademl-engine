package science.atlarge.grademl.input.airflow

import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import kotlin.system.exitProcess

class AirflowLogParser private constructor(
    private val airflowLogDirectory: Path
) {

    private lateinit var runId: String
    private lateinit var dagName: String
    private val taskNames = mutableSetOf<String>()
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
        return AirflowLog(runId, dagName, taskNames, taskDownstreamNames)
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
    val taskDownstreamNames: Map<String, Set<String>>
)

// Wrapper for testing the log parser
fun main(args: Array<String>) {
    if (args.size != 1 || args[0] == "--help") {
        println("Arguments: <airflowLogDirectory>")
        exitProcess(if (args.size != 1) -1 else 0)
    }

    val log = AirflowLogParser.parseFromDirectory(Paths.get(args[0]))
    println("Output of Airflow log parsing:")
    println("  Run ID:         ${log.runId}")
    println("  DAG name:       ${log.dagName}")
    println("  Tasks names:    ${log.taskNames.sorted().joinToString(", ")}")
    println("  Task order constraints:")
    if (log.taskDownstreamNames.isNotEmpty()) {
        log.taskDownstreamNames.entries.sortedBy { it.key }.forEach { (upstream, downstreams) ->
            println("    $upstream -> [${downstreams.joinToString(", ")}]")
        }
    } else {
        println("    (none)")
    }
}