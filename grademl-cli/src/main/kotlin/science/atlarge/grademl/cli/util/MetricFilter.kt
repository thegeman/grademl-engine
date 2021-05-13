package science.atlarge.grademl.cli.util

import science.atlarge.grademl.core.models.resource.Metric
import science.atlarge.grademl.core.models.resource.ResourceModel

class MetricFilter(
    resourceModel: ResourceModel,
    private val resourceFilter: ResourceFilter
) {

    // Inclusion list for metrics
    private val _includedMetrics = resourceModel.resources.flatMap { it.metrics }.toMutableSet()

    // Accessors for all metrics and included/excluded metrics
    val allMetrics: Set<Metric> = _includedMetrics.toSet()
    val includedMetrics: Set<Metric>
        get() = _includedMetrics.filter { it.resource in resourceFilter.includedResources }.toSet()
    val excludedMetrics: Set<Metric>
        get() = allMetrics - _includedMetrics

    fun includeMetrics(inclusions: Set<Metric>) {
        require(inclusions.all { it in allMetrics }) {
            "Cannot include metrics that are not part of this job's metric model"
        }
        // Include all given metrics
        _includedMetrics.addAll(inclusions)
    }

    fun excludeMetrics(exclusions: Set<Metric>) {
        require(exclusions.all { it in allMetrics }) {
            "Cannot exclude metrics that are not part of this job's metric model"
        }
        // Exclude all given metrics
        _includedMetrics.removeAll(exclusions)
    }

}