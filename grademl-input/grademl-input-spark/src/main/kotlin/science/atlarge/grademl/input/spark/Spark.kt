package science.atlarge.grademl.input.spark

import science.atlarge.grademl.core.execution.ExecutionModel
import science.atlarge.grademl.core.execution.ExecutionPhase
import science.atlarge.grademl.core.input.InputSource
import science.atlarge.grademl.core.resources.ResourceModel
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

        // TODO: Convert Spark logs to phases in the execution model
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