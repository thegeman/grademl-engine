package science.atlarge.grademl.cli

import org.jline.builtins.Completers
import org.jline.reader.LineReaderBuilder
import org.jline.reader.impl.DefaultParser
import org.jline.reader.impl.history.DefaultHistory
import org.jline.terminal.TerminalBuilder
import science.atlarge.grademl.cli.util.MetricList
import science.atlarge.grademl.cli.util.PhaseList
import science.atlarge.grademl.core.TimestampNs
import science.atlarge.grademl.core.execution.ExecutionModel
import science.atlarge.grademl.core.resources.Metric
import science.atlarge.grademl.core.resources.Resource
import science.atlarge.grademl.core.resources.ResourceModel
import science.atlarge.grademl.input.airflow.Airflow
import science.atlarge.grademl.input.resource_monitor.ResourceMonitor
import java.io.IOError
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.system.exitProcess

object Cli {

    @JvmStatic
    fun main(args: Array<String>) {
        println("Welcome to the GradeML CLI!")
        println()

        if (args.size != 2) {
            println("Usage: grademl-cli <jobLogDirectory> <jobAnalysisDirectory>")
            exitProcess(1)
        }

        println("Parsing job log files.")
        val (executionModel, resourceModel) = parseJobLogs(Paths.get(args[0]))
        println("Completed parsing of input files.")
        println()

        println("Explore the job's performance data interactively by issuing commands.")
        println("Enter \"help\" for a list of available commands.")
        println()

        runCli(CliState(executionModel, resourceModel, Paths.get(args[1])))
    }

    private fun parseJobLogs(jobLogDirectory: Path): Pair<ExecutionModel, ResourceModel> {
        val executionModel = ExecutionModel()
        val resourceModel = ResourceModel()

        Airflow.parseJobLogs(jobLogDirectory, executionModel)
        ResourceMonitor.parseJobLogs(jobLogDirectory, resourceModel)

        return executionModel to resourceModel
    }

    private fun runCli(cliState: CliState) {
        // Set up the terminal and CLI parsing library
        val terminal = TerminalBuilder.builder()
            .jansi(true)
            .build()
        val completer = Completers.AnyCompleter.INSTANCE
        val parser = DefaultParser()
        val lineReader = LineReaderBuilder.builder()
            .appName("GradeML")
            .terminal(terminal)
            .completer(completer)
            .parser(parser)
            .history(DefaultHistory())
            .build()

        // Repeatedly read, parse, and execute commands until the users quits the application
        while (true) {
            // Read the next line
            val line = try {
                lineReader.readLine("> ")
            } catch (e: Exception) {
                when (e) {
                    // End the CLI when the line reader is aborted
                    is IOError -> break
                    else -> throw e
                }
            }

            // For now, just echo back the parsed line
            val parsedLine = lineReader.parser.parse(line, 0)
            if (parsedLine.line().isBlank()) {
                // Skip empty lines
                continue
            }
            // Look up the first word as command
            val command = CommandRegistry[parsedLine.words()[0]]
            if (command == null) {
                println("Command \"${parsedLine.words()[0]}\" not recognized.")
                println()
                continue
            }
            // Invoke the command
            command.process(parsedLine.words().drop(1), cliState)
            println()
        }
    }

}

class CliState(
    val executionModel: ExecutionModel,
    val resourceModel: ResourceModel,
    val outputPath: Path
) {

    val phaseList = PhaseList.fromExecutionModel(executionModel)
    val metricList = MetricList.fromResourceModel(resourceModel)

    private val earliestTimestamp = executionModel.rootPhase.startTime

    fun normalizeTimestamp(plainTimestamp: TimestampNs): Long = plainTimestamp - earliestTimestamp
    fun denormalizeTimestamp(normalizedTimestamp: Long): TimestampNs = normalizedTimestamp + earliestTimestamp

    // Exclusion list for resources
    private val excludedResources = mutableSetOf<Resource>()

    // Accessors for non-excluded resources (default) and all resources
    val selectedResources: Set<Resource>
        get() = allResources - excludedResources
    val allResources: Set<Resource> = resourceModel.resources - resourceModel.rootResource

    fun excludeResources(exclusions: Set<Resource>) {
        require(exclusions.all { it in resourceModel.resources }) {
            "Cannot exclude resources that are not part of this job's resource model"
        }
        // Exclude all given resources and any children
        val allExclusions = mutableSetOf<Resource>()
        val exclusionsToCheck = mutableListOf<Resource>()
        allExclusions.addAll(exclusions)
        exclusionsToCheck.addAll(exclusions)
        while (exclusionsToCheck.isNotEmpty()) {
            val nextToCheck = exclusionsToCheck.removeLast()
            val newExclusions = nextToCheck.children - allExclusions
            allExclusions.addAll(newExclusions)
            exclusionsToCheck.addAll(newExclusions)
        }
        excludedResources.addAll(allExclusions.filter { !it.isRoot })
    }

    fun includeResources(inclusions: Set<Resource>) {
        require(inclusions.all { it in resourceModel.resources }) {
            "Cannot include resources that are not part of this job's resource model"
        }
        // Include all given resources and any parents
        val allInclusions = mutableSetOf<Resource>()
        val inclusionsToCheck = mutableListOf<Resource>()
        allInclusions.addAll(inclusions)
        inclusionsToCheck.addAll(inclusions)
        while (inclusionsToCheck.isNotEmpty()) {
            val nextToCheck = inclusionsToCheck.removeLast()
            val parent = nextToCheck.parent ?: continue
            if (parent !in allInclusions) {
                allInclusions.add(parent)
                inclusionsToCheck.add(parent)
            }
        }
        excludedResources.removeAll(allInclusions)
    }

    // Exclusion list for metrics
    private val excludedMetrics = mutableSetOf<Metric>()

    // Accessors for non-excluded metrics (default) and all metrics
    val selectedMetrics: Set<Metric>
        get() = allMetrics - excludedMetrics
    val allMetrics: Set<Metric> = resourceModel.resources.flatMap { it.metrics }.toSet()

    fun excludeMetrics(exclusions: Set<Metric>) {
        require(exclusions.all { it in allMetrics }) {
            "Cannot exclude metrics that are not part of this job's resource model"
        }
        // Exclude all given metrics
        excludedMetrics.addAll(exclusions)
    }

    fun includeMetrics(inclusions: Set<Metric>) {
        require(inclusions.all { it in allMetrics }) {
            "Cannot include metrics that are not part of this job's resource model"
        }
        // Include all given metrics and corresponding resources
        excludedMetrics.removeAll(inclusions)
        includeResources(inclusions.map { it.resource }.toSet())
    }

}