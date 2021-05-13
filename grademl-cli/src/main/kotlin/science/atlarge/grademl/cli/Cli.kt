package science.atlarge.grademl.cli

import org.jline.reader.EndOfFileException
import science.atlarge.grademl.cli.terminal.GradeMLTerminal
import science.atlarge.grademl.cli.util.*
import science.atlarge.grademl.core.GradeMLEngine
import science.atlarge.grademl.core.GradeMLJob
import science.atlarge.grademl.core.GradeMLJobStatusUpdate
import science.atlarge.grademl.core.attribution.ResourceAttribution
import science.atlarge.grademl.core.models.execution.ExecutionModel
import science.atlarge.grademl.core.models.resource.ResourceModel
import science.atlarge.grademl.input.airflow.Airflow
import science.atlarge.grademl.input.resource_monitor.ResourceMonitor
import science.atlarge.grademl.input.spark.Spark
import java.io.File
import java.io.IOError
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.system.exitProcess

object Cli {

    @JvmStatic
    fun main(args: Array<String>) {
        val terminal = GradeMLTerminal()
        terminal.setWindowTitle("GradeML CLI")

        println("Welcome to the GradeML CLI!")
        println()

        if (args.size != 2) {
            println("Usage: grademl-cli <jobLogDirectories> <jobAnalysisDirectory>")
            println("  jobLogDirectories may be separated by the ${File.pathSeparatorChar} character")
            exitProcess(1)
        }

        val inputPaths = args[0].split(File.pathSeparatorChar).map { Paths.get(it) }
        val outputPath = Paths.get(args[1])

        GradeMLEngine.registerInputSource(ResourceMonitor)
        GradeMLEngine.registerInputSource(Spark)
        GradeMLEngine.registerInputSource(Airflow)

        val gradeMLJob = GradeMLEngine.analyzeJob(inputPaths, outputPath) { update ->
            when (update) {
                GradeMLJobStatusUpdate.LOG_PARSING_STARTING -> {
                    println("Parsing job log files.")
                }
                GradeMLJobStatusUpdate.LOG_PARSING_COMPLETED -> {
                    println("Completed parsing of input files.")
                    println()
                }
                else -> {
                }
            }
        }

        terminal.setActiveJob(gradeMLJob)

        if (
            gradeMLJob.unifiedExecutionModel.phases.size == 1 &&
            gradeMLJob.unifiedResourceModel.resources.any { it.metrics.isNotEmpty() }
        ) {
            println(
                "Did not find any execution logs. " +
                        "Creating dummy execution model to allow analysis of the resource model."
            )
            println()
            val (startTime, endTime) = gradeMLJob.unifiedResourceModel.resources.flatMap { it.metrics }
                .map { it.data.timestamps.first() to it.data.timestamps.last() }
                .reduce { acc, pair -> minOf(acc.first, pair.first) to maxOf(acc.second, pair.second) }
            gradeMLJob.unifiedExecutionModel.addPhase("dummy_phase", startTime = startTime, endTime = endTime)
        }

        println("Explore the job's performance data interactively by issuing commands.")
        println("Enter \"help\" for a list of available commands.")
        println()

        runCli(CliState(gradeMLJob, outputPath), terminal)
    }

    private fun runCli(cliState: CliState, terminal: GradeMLTerminal) {
        // Repeatedly read, parse, and execute commands until the users quits the application
        while (true) {
            // Read the next line
            val parsedLine = try {
                terminal.readAndParseLine()
            } catch (e: Exception) {
                when (e) {
                    // End the CLI when the line reader is aborted
                    is IOError -> break
                    is EndOfFileException -> break
                    else -> throw e
                }
            }

            // Look up the first word as command
            val command = CommandRegistry[parsedLine[0]]
            if (command == null) {
                println("Command \"${parsedLine[0]}\" not recognized.")
                println()
                continue
            }

            // Invoke the command
            command.process(parsedLine.drop(1), cliState)
            println()
        }
    }

}

class CliState(
    private val gradeMLJob: GradeMLJob,
    val outputPath: Path
) {

    val executionModel: ExecutionModel
        get() = gradeMLJob.unifiedExecutionModel
    val resourceModel: ResourceModel
        get() = gradeMLJob.unifiedResourceModel
    val resourceAttribution: ResourceAttribution
        get() = gradeMLJob.resourceAttribution

    val phaseList = PhaseList.fromExecutionModel(executionModel)
    val phaseTypeList = PhaseTypeList.fromExecutionModel(executionModel)
    val metricList = MetricList.fromResourceModel(resourceModel)

    val phaseFilter = PhaseFilter(executionModel)
    val resourceFilter = ResourceFilter(resourceModel)
    val metricFilter = MetricFilter(resourceModel, resourceFilter)

    val time = JobTime(executionModel)
    val output = OutputPaths(outputPath, phaseList, metricList)
        .also { it.writeIndex(executionModel, resourceModel, time) }

}