package science.atlarge.grademl.query.experirments

import science.atlarge.grademl.core.GradeMLEngine
import science.atlarge.grademl.core.attribution.AttributedResourceData
import science.atlarge.grademl.core.attribution.NoAttributedData
import science.atlarge.grademl.core.attribution.ResourceAttributionSettings
import science.atlarge.grademl.core.attribution.ResourceDemandEstimate
import science.atlarge.grademl.core.models.PathMatchException
import science.atlarge.grademl.core.models.PathMatchResult
import science.atlarge.grademl.core.models.PathMatches
import science.atlarge.grademl.core.models.resource.MetricPath
import science.atlarge.grademl.core.models.resource.ResourcePath
import java.nio.file.Path
import kotlin.system.measureNanoTime

fun runResourceAttributionOverheadExperiment(inputPaths: List<Path>, outputPath: Path) {
    val enableTimeSeriesCompression = false

    println("STARTING EXPERIMENT -- Measure Resource Attribution Overhead")
    println("SETTING: enableTimeSeriesCompression = $enableTimeSeriesCompression")

    // Analyze the job input
    val gradeMLJob = GradeMLEngine.analyzeJob(
        inputPaths, outputPath,
        ResourceAttributionSettings(
            enableTimeSeriesCompression = enableTimeSeriesCompression,
            enableRuleCaching = false
        )
    )

    // Create an output directory for storing experiment results
    val expOutputPath = outputPath.resolve("experiment_resource_attribution_overhead")
    expOutputPath.toFile().mkdirs()

    // Experiment design
    // Key measurements for several resources (1 CPU, 1 disk, 1 network)
    // - time to compute attribution rules
    // - time to perform attribution to all phases
    // - input data size
    // - intermediate data size
    // - output data size
    // Estimate:
    // - impact of caching optimization
    // - impact of lazy evaluation

    // Select relevant metrics
    val cpuMetrics = gradeMLJob.unifiedResourceModel
        .resolvePath(MetricPath(ResourcePath.parse("/cluster/machine/cpu"), "utilization"))
        .let { when (it) {
            is PathMatchException -> throw IllegalArgumentException("No CPU metrics found")
            is PathMatches -> it.matches.toSet()
        } }
    val networkMetrics = gradeMLJob.unifiedResourceModel
        .resolvePath(ResourcePath.parse("/cluster/machine/network"))
        .let { when (it) {
            is PathMatchException -> throw IllegalArgumentException("No network metrics found")
            is PathMatches -> it.matches.toSet()
        } }
        .filter { resource ->
            resource.tags["iface"] == "eth0" || resource.tags["iface"] == "ib0"
        }
        .flatMap { iface ->
            listOf(iface.metricsByName["bytes-sent"]!!, iface.metricsByName["bytes-received"]!!)
        }
    val diskMetrics = gradeMLJob.unifiedResourceModel
        .resolvePath(ResourcePath.parse("/cluster/machine/disk"))
        .let { when (it) {
            is PathMatchException -> throw IllegalArgumentException("No network metrics found")
            is PathMatches -> it.matches.toSet()
        } }
        .filter { resource ->
            resource.tags["device"] == "md0" || resource.tags["device"] == "sda" || resource.tags["device"] == "sdb"
        }
        .flatMap { drive ->
            if (drive.tags["device"] == "md0") {
                listOf(drive.metricsByName["bytes-read"]!!, drive.metricsByName["bytes-written"]!!)
            } else {
                listOf(drive.metricsByName["total-utilization"]!!)
            }
        }

    // Perform the demand estimation step
    // Randomize the order of metrics to even out cold-start bias over multiple runs
    val allMetrics = (cpuMetrics.map { "CPU" to it } + networkMetrics.map { "network" to it } + diskMetrics.map { "disk" to it }).shuffled()
//    val allMetrics = (cpuMetrics.map { "CPU" to it }).shuffled()
    var totalTimeTakenDemandEstimate = 0L
    var totalInputDataPoints = 0L
    for ((metricType, metric) in allMetrics) {
        println("Estimating demand for $metricType resource: ${metric.path.asPlainPath}")
        val estimatedDemand: ResourceDemandEstimate?
        val timeTaken = measureNanoTime {
            estimatedDemand = gradeMLJob.resourceAttribution.estimateDemand(metric)
        }
        totalTimeTakenDemandEstimate += timeTaken
        println("  Time taken: ${String.format("%.3f", timeTaken / 1_000_000.0)} ms")
        if (estimatedDemand != null) {
            val sampleCount = estimatedDemand.variableDemandOverTime.values.size
            println("  Produced $sampleCount samples from ${metric.data.values.size} measurements")
        }
        totalInputDataPoints += metric.data.values.size
    }

    // Perform the upsampling step
    var totalTimeTakenUpsampling = 0L
    var totalIntermediateDataPoints = 0L
    for ((metricType, metric) in allMetrics) {
        println("Upsampling $metricType resource: ${metric.path.asPlainPath}")
        val timeTaken = measureNanoTime {
            totalIntermediateDataPoints += gradeMLJob.resourceAttribution.upsampleMetric(metric)!!.values.size
        }
        totalTimeTakenUpsampling += timeTaken
        println("  Time taken: ${String.format("%.3f", timeTaken / 1_000_000.0)} ms")
    }

    // Perform the attribution step
    var totalOutputDataPoints = 0L
    var totalTimeTakenAttribution = 0L
    var totalAttributionRules = 0
    var nonNullAttributionRules = 0
    for ((metricType, metric) in allMetrics) {
        println("Attributing utilization of $metricType resource: ${metric.path.asPlainPath}")
        var dataPointCount = 0L
        var phaseCount = 0
        val timeTaken = measureNanoTime {
            for (phase in gradeMLJob.resourceAttribution.leafPhases) {
                totalAttributionRules++
                when (val attr = gradeMLJob.resourceAttribution.attributeMetricToPhase(metric, phase)) {
                    is AttributedResourceData -> {
                        nonNullAttributionRules++
                        dataPointCount += attr.metricData.values.size
                        phaseCount++
                    }
                }
            }
        }
        totalOutputDataPoints += dataPointCount
        totalTimeTakenAttribution += timeTaken
        println("  Time taken: ${String.format("%.3f", timeTaken / 1_000_000.0)} ms")
        println("  Produced $dataPointCount samples for $phaseCount phases")
    }

    println()
    println("Time for demand estimation: ${String.format("%.3f", totalTimeTakenDemandEstimate / 1_000_000.0)} ms")
    println("Time for upsampling: ${String.format("%.3f", totalTimeTakenUpsampling / 1_000_000.0)} ms")
    println("Time for resource attribution: ${String.format("%.3f", totalTimeTakenAttribution / 1_000_000.0)} ms")
    println("Number of metrics: ${allMetrics.size} (${cpuMetrics.size} CPU, ${networkMetrics.size} network, ${diskMetrics.size} disk)")
    println("Total input data points: $totalInputDataPoints")
    println("Total intermediate data points: $totalIntermediateDataPoints")
    println("Total attribution rules: $totalAttributionRules")
    println("Non-null attribution rules: $nonNullAttributionRules")
    println("Total output data points: $totalOutputDataPoints")
}