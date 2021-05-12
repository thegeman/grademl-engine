package science.atlarge.grademl.cli

import org.jline.reader.EndOfFileException
import science.atlarge.grademl.cli.terminal.GradeMLTerminal
import science.atlarge.grademl.cli.util.MetricList
import science.atlarge.grademl.cli.util.PhaseList
import science.atlarge.grademl.cli.util.PhaseTypeList
import science.atlarge.grademl.core.GradeMLEngine
import science.atlarge.grademl.core.GradeMLJob
import science.atlarge.grademl.core.GradeMLJobStatusUpdate
import science.atlarge.grademl.core.TimestampNs
import science.atlarge.grademl.core.attribution.ResourceAttribution
import science.atlarge.grademl.core.execution.ExecutionModel
import science.atlarge.grademl.core.execution.ExecutionPhase
import science.atlarge.grademl.core.resources.Metric
import science.atlarge.grademl.core.resources.Resource
import science.atlarge.grademl.core.resources.ResourceModel
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

    private val earliestTimestamp = executionModel.rootPhase.startTime

    fun normalizeTimestamp(plainTimestamp: TimestampNs): Long = plainTimestamp - earliestTimestamp
    fun denormalizeTimestamp(normalizedTimestamp: Long): TimestampNs = normalizedTimestamp + earliestTimestamp

    private val rootPhaseOutputPath = outputPath.resolve("root_phase")
    fun outputPathForPhase(phase: ExecutionPhase): Path =
        if (phase.isRoot) {
            rootPhaseOutputPath
        } else {
            phase.path.pathComponents.fold(rootPhaseOutputPath) { acc, pathComponent ->
                acc.resolve(pathComponent)
            }
        }

    private val rootResourceOutputPath = outputPath.resolve("root_resource")
    fun outputPathForResource(resource: Resource): Path =
        if (resource.isRoot) {
            rootResourceOutputPath
        } else {
            resource.path.pathComponents.fold(rootResourceOutputPath) { acc, pathComponent ->
                acc.resolve(pathComponent)
            }
        }

    fun outputPathForMetric(metric: Metric): Path = outputPathForResource(metric.resource).resolve(metric.name)

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