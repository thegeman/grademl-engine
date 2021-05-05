package science.atlarge.grademl.core.attribution

sealed class ResourceAttributionRule {
    object None : ResourceAttributionRule()
    data class Exact(val demand: Double) : ResourceAttributionRule()
    data class Variable(val demand: Double) : ResourceAttributionRule()
}