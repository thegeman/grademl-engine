package science.atlarge.grademl.cli

import science.atlarge.grademl.core.execution.ExecutionModel
import science.atlarge.grademl.core.resources.ResourceModel
import science.atlarge.grademl.input.airflow.Airflow
import science.atlarge.grademl.input.resource_monitor.ResourceMonitor
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.system.exitProcess

object Cli {

    @JvmStatic
    fun main(args: Array<String>) {
        println("Welcome to the GradeML CLI!")
        println()

        if (args.size != 1) {
            println("Usage: grademl-cli <jobLogDirectory>")
            exitProcess(1)
        }

        println("Parsing job log files.")
        val (executionModel, resourceModel) = parseJobLogs(Paths.get(args[0]))
        println("Completed parsing of input files.")
        println()

        println("Explore the job's performance data interactively by issuing commands.")
        println("Enter \"help\" for a list of available commands.")
        println()

        runCli(executionModel, resourceModel)
    }

    private fun parseJobLogs(jobLogDirectory: Path): Pair<ExecutionModel, ResourceModel> {
        val executionModel = ExecutionModel()
        val resourceModel = ResourceModel()

        Airflow.parseJobLogs(jobLogDirectory, executionModel)
        ResourceMonitor.parseJobLogs(jobLogDirectory, resourceModel)

        return executionModel to resourceModel
    }

    private fun runCli(executionModel: ExecutionModel, resourceModel: ResourceModel) {
        TODO()
    }

}