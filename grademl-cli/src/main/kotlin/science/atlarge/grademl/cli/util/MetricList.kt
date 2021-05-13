package science.atlarge.grademl.cli.util

import science.atlarge.grademl.core.models.resource.Metric
import science.atlarge.grademl.core.models.resource.Resource
import science.atlarge.grademl.core.models.resource.ResourceModel

// Create a deterministic mapping from a resource model's metrics to internally unique identifiers
class MetricList private constructor(
    metricOrder: List<Metric>
) {

    private val metricToId: Map<Metric, String>
    private val idToMetric: Map<String, Metric>

    init {
        val metricCount = metricOrder.size
        val metricCountDigits = metricCount.toString().length
        val metricIdFormat = "m%0${metricCountDigits}d"
        metricToId = metricOrder.withIndex().associate { it.value to metricIdFormat.format(it.index) }
        idToMetric = metricToId.entries.associate { it.value to it.key }
    }

    fun metricToIdentifier(metric: Metric): String {
        require(metric in metricToId) { "Unknown metric: \"${metric.path}\"" }
        return metricToId[metric]!!
    }

    fun identifierToMetric(metricIdentifier: String): Metric {
        require(metricIdentifier in idToMetric) { "Unknown metric identifier: \"$metricIdentifier\"" }
        return idToMetric[metricIdentifier]!!
    }

    companion object {

        fun fromResourceModel(resourceModel: ResourceModel): MetricList {
            // Order all resources in the resource model using a DFS
            val resourceOrder = mutableListOf<Resource>()
            appendSubtreeInOrder(resourceModel.rootResource, resourceOrder)
            // Map each resource to its metrics to obtain a metric order
            val metricOrder = resourceOrder.flatMap { res -> res.metrics.sortedBy { it.name } }
            return MetricList(metricOrder)
        }

        private fun appendSubtreeInOrder(resource: Resource, resourceOrder: MutableList<Resource>) {
            resourceOrder.add(resource)
            for (child in resource.children.sortedBy { it.identifier }) {
                appendSubtreeInOrder(child, resourceOrder)
            }
        }

    }

}