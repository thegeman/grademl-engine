package science.atlarge.grademl.query.experirments

import science.atlarge.grademl.core.GradeMLEngine
import science.atlarge.grademl.core.GradeMLJobStatusUpdate
import science.atlarge.grademl.core.attribution.ResourceAttributionSettings
import java.nio.file.Path

fun exportJobCharacteristics(inputPaths: List<Path>, outputPath: Path) {
    println("STARTING EXPERIMENT -- Export Job Characteristics")

    // Analyze the job input
    var logParsingStartTime = 0L
    val gradeMLJob = GradeMLEngine.analyzeJob(inputPaths, outputPath, ResourceAttributionSettings(
        enableRuleCaching = false
    )) { update ->
        when (update) {
            GradeMLJobStatusUpdate.LOG_PARSING_STARTING -> {
                logParsingStartTime = System.nanoTime()
            }
            GradeMLJobStatusUpdate.LOG_PARSING_COMPLETED -> {
                val logParsingEndTime = System.nanoTime()
                println("Time to parse logs: ${String.format("%.2f", (logParsingEndTime - logParsingStartTime) / 1_000_000.0)} ms")
            }
            else -> {
            }
        }
    }

    // Create an output directory for storing experiment results
    val expOutputPath = outputPath.resolve("experiment_job_characteristics")
    expOutputPath.toFile().mkdirs()

    // Extract basic characteristics
    val jobDuration = gradeMLJob.unifiedExecutionModel.rootPhase.duration
    val phaseDuration = gradeMLJob.unifiedExecutionModel.phases.sumOf { it.duration }
    val phaseCount = gradeMLJob.unifiedExecutionModel.phases.size
    val dependencyCount = gradeMLJob.unifiedExecutionModel.phases.sumOf { it.outFlows.size }
    val metrics = gradeMLJob.unifiedResourceModel.resources.flatMap { it.metrics }
    val metricCount = metrics.size

    // Determine metric sizes
    val metricSizes = metrics.map { metric ->
        metric to metric.data.values.size
    }
    val totalMetricValues = metricSizes.sumOf { it.second.toLong() }
    val uniqueMetricTimeSeries = metrics.map { metric -> metric.data.timestamps }.toSet()
    val totalTimestampCount = uniqueMetricTimeSeries.sumOf { it.size.toLong() }

    // Print basic job characteristics to the terminal
    println("Job characteristics:")
    println("  Job duration: ${String.format("%.2f", jobDuration / 1_000_000_000.0)} s")
    println("  Phase duration: ${String.format("%.2f", phaseDuration / 1_000_000_000.0)} s")
    println("  Phase count: $phaseCount")
    println("  Dependency count: $dependencyCount")
    println("  Metric count: $metricCount")
    println("  Metric data points: $totalMetricValues")
    println("  Metric timestamp count: $totalTimestampCount")
    println("  Metric memory footprint: ${String.format("%.2f", 8 * (totalMetricValues + totalTimestampCount) / 1024.0 / 1024.0)} MiB")

    // Write detailed job characteristics to a file

}
