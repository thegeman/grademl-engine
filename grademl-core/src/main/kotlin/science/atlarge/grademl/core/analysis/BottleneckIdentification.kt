package science.atlarge.grademl.core.analysis

import science.atlarge.grademl.core.DurationNs
import science.atlarge.grademl.core.FractionalDurationNs
import science.atlarge.grademl.core.attribution.AttributedResourceData
import science.atlarge.grademl.core.attribution.ResourceAttribution
import science.atlarge.grademl.core.models.execution.ExecutionPhase
import science.atlarge.grademl.core.models.resource.Metric
import science.atlarge.grademl.core.models.resource.MetricPath
import science.atlarge.grademl.core.models.resource.Resource
import science.atlarge.grademl.core.models.resource.ResourcePath

class BottleneckIdentification(
    private val resourceAttribution: ResourceAttribution
) {

    fun analyzePhase(phase: ExecutionPhase, selectedMetrics: Set<Metric>): PhaseBottlenecks {
        val attributionDataForPhase = selectedMetrics
            .map { it to resourceAttribution.attributeMetricToPhase(it, phase) }
            .filter { it.second is AttributedResourceData }
            .associate { it.first to it.second as AttributedResourceData }

        // Determine bottlenecks per metric
        val metricOrder = attributionDataForPhase.keys.toList()
        val metricBottlenecks = BottleneckIdentificationStep.analyzeMetricAttributionData(
            metricOrder.map { setOf(it) },
            attributionDataForPhase
        )
        // Determine bottlenecks per resource
        val resourceOrderAndGrouping = metricOrder.groupBy { it.resource }.toList()
        val resourceBottlenecks = BottleneckIdentificationStep.analyzeMetricAttributionData(
            resourceOrderAndGrouping.map { it.second.toSet() },
            attributionDataForPhase
        )
        // Determine bottlenecks per metric type
        val metricTypeOrderAndGrouping = metricOrder.groupBy { it.type }.toList()
        val metricTypeBottlenecks = BottleneckIdentificationStep.analyzeMetricAttributionData(
            metricTypeOrderAndGrouping.map { it.second.toSet() },
            attributionDataForPhase
        )
        // Determine bottlenecks per resource type
        val resourceTypeOrderAndGrouping = metricOrder.groupBy { it.resource.type }.toList()
        val resourceTypeBottlenecks = BottleneckIdentificationStep.analyzeMetricAttributionData(
            resourceTypeOrderAndGrouping.map { it.second.toSet() },
            attributionDataForPhase
        )

        return PhaseBottlenecks(
            phase,
            phase.duration,
            resourceTypeBottlenecks.durationWithoutOverhead,
            durationWithoutMetric = metricOrder.mapIndexed { i, m ->
                m to metricBottlenecks.durationWithoutMetricGroup[i]
            }.toMap(),
            durationWithoutMetricAndOverhead = metricOrder.mapIndexed { i, m ->
                m to metricBottlenecks.durationWithoutMetricGroupAndOverhead[i]
            }.toMap(),
            durationWithoutResource = resourceOrderAndGrouping.mapIndexed { i, rg ->
                rg.first to resourceBottlenecks.durationWithoutMetricGroup[i]
            }.toMap(),
            durationWithoutResourceAndOverhead = resourceOrderAndGrouping.mapIndexed { i, rg ->
                rg.first to resourceBottlenecks.durationWithoutMetricGroupAndOverhead[i]
            }.toMap(),
            durationWithoutMetricType = metricTypeOrderAndGrouping.mapIndexed { i, rg ->
                rg.first to metricTypeBottlenecks.durationWithoutMetricGroup[i]
            }.toMap(),
            durationWithoutMetricTypeAndOverhead = metricTypeOrderAndGrouping.mapIndexed { i, rg ->
                rg.first to metricTypeBottlenecks.durationWithoutMetricGroupAndOverhead[i]
            }.toMap(),
            durationWithoutResourceType = resourceTypeOrderAndGrouping.mapIndexed { i, rg ->
                rg.first to resourceTypeBottlenecks.durationWithoutMetricGroup[i]
            }.toMap(),
            durationWithoutResourceTypeAndOverhead = resourceTypeOrderAndGrouping.mapIndexed { i, rg ->
                rg.first to resourceTypeBottlenecks.durationWithoutMetricGroupAndOverhead[i]
            }.toMap()
        )
    }



}

class PhaseBottlenecks(
    val phase: ExecutionPhase,
    val observedDuration: DurationNs,
    val durationWithoutOverhead: FractionalDurationNs,
    val durationWithoutMetric: Map<Metric, FractionalDurationNs>,
    val durationWithoutMetricAndOverhead: Map<Metric, FractionalDurationNs>,
    val durationWithoutMetricType: Map<MetricPath, FractionalDurationNs>,
    val durationWithoutMetricTypeAndOverhead: Map<MetricPath, FractionalDurationNs>,
    val durationWithoutResource: Map<Resource, FractionalDurationNs>,
    val durationWithoutResourceAndOverhead: Map<Resource, FractionalDurationNs>,
    val durationWithoutResourceType: Map<ResourcePath, FractionalDurationNs>,
    val durationWithoutResourceTypeAndOverhead: Map<ResourcePath, FractionalDurationNs>
)