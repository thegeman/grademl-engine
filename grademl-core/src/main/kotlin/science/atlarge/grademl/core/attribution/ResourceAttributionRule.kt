package science.atlarge.grademl.core.attribution

import science.atlarge.grademl.core.models.execution.ExecutionPhase
import science.atlarge.grademl.core.models.resource.Metric

sealed class ResourceAttributionRule {
    object None : ResourceAttributionRule()
    data class Exact(val demand: Double) : ResourceAttributionRule()
    data class Variable(val demand: Double) : ResourceAttributionRule()
}

interface ResourceAttributionRuleProvider {
    fun forPhaseAndMetric(phase: ExecutionPhase, metric: Metric): ResourceAttributionRule?
}

object DefaultResourceAttributionRuleProvider : ResourceAttributionRuleProvider {
    override fun forPhaseAndMetric(phase: ExecutionPhase, metric: Metric) = ResourceAttributionRule.Variable(1.0)
}