package science.atlarge.grademl.core.attribution

import science.atlarge.grademl.core.models.execution.ExecutionModel
import science.atlarge.grademl.core.models.execution.ExecutionPhase
import science.atlarge.grademl.core.models.resource.Metric
import science.atlarge.grademl.core.models.resource.MetricData
import science.atlarge.grademl.core.models.resource.ResourceModel

class ResourceAttribution(
    executionModel: ExecutionModel,
    resourceModel: ResourceModel,
    attributionRuleProvider: ResourceAttributionRuleProvider
) {

    val leafPhases = executionModel.rootPhase.descendants.filter { it.children.isEmpty() }.toSet()
    val metrics = resourceModel.rootResource.metricsInTree

    private val demandEstimationStep = ResourceDemandEstimationStep(
        metrics,
        leafPhases,
        attributionRuleProvider
    )

    private val upsamplingStep = ResourceUpsamplingStep(
        metrics
    ) { metric -> demandEstimationStep.estimatedDemandForMetric(metric)!! }

    private val attributionStep = ResourceAttributionStep(
        leafPhases,
        metrics,
        attributionRuleProvider,
        { metric -> demandEstimationStep.estimatedDemandForMetric(metric)!! },
        { metric -> upsamplingStep.upsampleMetric(metric)!! }
    )

    fun attributeMetricToPhase(metric: Metric, phase: ExecutionPhase): ResourceAttributionResult {
        return attributionStep.attributeMetricToPhase(metric, phase)
    }

    fun upsampleMetric(metric: Metric): MetricData? {
        return upsamplingStep.upsampleMetric(metric)
    }

    fun estimateDemand(metric: Metric): ResourceDemandEstimate? {
        return demandEstimationStep.estimatedDemandForMetric(metric)
    }

}

sealed class ResourceAttributionResult

class AttributedResourceData(
    val metricData: MetricData,
    val availableCapacity: MetricData
) : ResourceAttributionResult()

object NoAttributedData : ResourceAttributionResult()