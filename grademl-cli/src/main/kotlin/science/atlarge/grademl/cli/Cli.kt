package science.atlarge.grademl.cli

import org.jline.reader.EndOfFileException
import org.jline.reader.LineReaderBuilder
import org.jline.reader.impl.DefaultParser
import org.jline.reader.impl.history.DefaultHistory
import org.jline.terminal.TerminalBuilder
import science.atlarge.grademl.cli.terminal.CommandCompleter
import science.atlarge.grademl.cli.util.MetricList
import science.atlarge.grademl.cli.util.PhaseList
import science.atlarge.grademl.core.TimestampNs
import science.atlarge.grademl.core.execution.ExecutionModel
import science.atlarge.grademl.core.execution.ExecutionPhase
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

        if (executionModel.phases.size == 1 && resourceModel.resources.any { it.metrics.isNotEmpty() }) {
            println(
                "Did not find any execution logs. Creating dummy execution model to allow " +
                        "analysis of the resource model."
            )
            executionModel.addPhase(
                "dummy_phase",
                startTime = resourceModel.resources.flatMap { it.metrics }.minOf { it.data.timestamps.first() },
                endTime = resourceModel.resources.flatMap { it.metrics }.maxOf { it.data.timestamps.last() }
            )
        }

        return executionModel to resourceModel
    }

    private fun runCli(cliState: CliState) {
        // Set up the terminal and CLI parsing library
        val terminal = TerminalBuilder.builder()
            .jansi(true)
            .build()
        val parser = DefaultParser()
        val lineReader = LineReaderBuilder.builder()
            .appName("GradeML")
            .terminal(terminal)
            .completer(CommandCompleter(cliState))
            .parser(parser)
            .history(DefaultHistory())
            .build()

        // Set window title
        terminal.writer().println("\u001B]0;GradeML CLI\u0007")

        // Repeatedly read, parse, and execute commands until the users quits the application
        while (true) {
            // Read the next line
            val line = try {
                lineReader.readLine("> ")
            } catch (e: Exception) {
                when (e) {
                    // End the CLI when the line reader is aborted
                    is IOError -> break
                    is EndOfFileException -> break
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

    private val rootPhaseOutputPath = outputPath.resolve("root_phase")
    fun outputPathForPhase(phase: ExecutionPhase) =
        if (phase.isRoot) {
            rootPhaseOutputPath
        } else {
            phase.path.pathComponents.fold(rootPhaseOutputPath) { acc, pathComponent -> acc.resolve(pathComponent) }
        }

    // Exclusion list for phases
    private val excludedPhases = mutableSetOf<ExecutionPhase>()

    // Accessors for non-excluded phases (default) and all phases
    val selectedPhases: Set<ExecutionPhase>
        get() = allPhases - excludedPhases
    val allPhases: Set<ExecutionPhase> = executionModel.phases - executionModel.rootPhase

    fun excludePhases(exclusions: Set<ExecutionPhase>) {
        require(exclusions.all { it in executionModel.phases }) {
            "Cannot exclude phases that are not part of this job's execution model"
        }
        // Exclude all given phases and any children
        val allExclusions = mutableSetOf<ExecutionPhase>()
        val exclusionsToCheck = mutableListOf<ExecutionPhase>()
        allExclusions.addAll(exclusions)
        exclusionsToCheck.addAll(exclusions)
        while (exclusionsToCheck.isNotEmpty()) {
            val nextToCheck = exclusionsToCheck.removeLast()
            val newExclusions = nextToCheck.children - allExclusions
            allExclusions.addAll(newExclusions)
            exclusionsToCheck.addAll(newExclusions)
        }
        excludedPhases.addAll(allExclusions.filter { !it.isRoot })
    }

    fun includePhases(inclusions: Set<ExecutionPhase>) {
        require(inclusions.all { it in executionModel.phases }) {
            "Cannot include phases that are not part of this job's execution model"
        }
        // Include all given phases and any parents
        val allInclusions = mutableSetOf<ExecutionPhase>()
        val inclusionsToCheck = mutableListOf<ExecutionPhase>()
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
        excludedPhases.removeAll(allInclusions)
    }
    
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
        get() = allMetrics.filter { it !in excludedMetrics && it.resource !in excludedResources }.toSet()
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