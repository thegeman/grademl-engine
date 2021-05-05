package science.atlarge.grademl.core.attribution

import science.atlarge.grademl.core.execution.ExecutionPhase
import science.atlarge.grademl.core.resources.Metric

sealed class ResourceAttributionRule {
    object None : ResourceAttributionRule()
    data class Exact(val demand: Double) : ResourceAttributionRule()
    data class Variable(val demand: Double) : ResourceAttributionRule()
}

typealias ResourceAttributionProvider = (ExecutionPhase, Metric) -> ResourceAttributionRule

object ResourceAttributionRuleProviders {

    val DEFAULT: ResourceAttributionProvider = { _, _ -> ResourceAttributionRule.Variable(1.0) }

}