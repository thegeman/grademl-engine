package science.atlarge.grademl.core.resources

class ResourceModel {

    // Collection of resources
    val rootResource: Resource = RootResource(this)
    private val _resources = mutableSetOf<Resource>(rootResource)
    val resources: Set<Resource>
        get() = _resources

    // Parent-child relationships
    private val resourceParents = mutableMapOf<Resource, Resource>()
    private val resourceChildren = mutableMapOf<Resource, MutableSet<Resource>>()

    fun getParentOf(resource: Resource): Resource? = resourceParents[resource]
    fun getChildrenOf(resource: Resource): Set<Resource> = resourceChildren[resource] ?: emptySet()

    fun addResource(
        name: String,
        tags: Map<String, String> = emptyMap(),
        description: String? = null,
        parent: Resource = rootResource
    ): Resource {
        require(parent in _resources) { "Cannot add resource with parent that is not part of this ResourceModel" }
        val resource = SubResource(name, tags, description, this)
        _resources.add(resource)
        resourceParents[resource] = parent
        resourceChildren.getOrPut(parent) { mutableSetOf() }.add(resource)
        return resource
    }

}

sealed class Resource(
    val name: String,
    val tags: Map<String, String>,
    val description: String?,
    private val model: ResourceModel
) {

    private val _metrics = mutableSetOf<Metric>()
    val metrics: Set<Metric>
        get() = _metrics

    private val _metricsByName = mutableMapOf<String, Metric>()
    val metricsByName: Map<String, Metric>
        get() = _metricsByName

    val identifier: String by lazy {
        if (tags.isEmpty()) name
        else "$name[${
            tags.entries.sortedBy { it.key }
                .joinToString(separator = ", ") { "${it.key}=${it.value}" }
        }]"
    }

    val path: String by lazy {
        when {
            isRoot -> "/"
            parent!!.isRoot -> "/$identifier"
            else -> "${parent!!.path}/$identifier"
        }
    }

    val isRoot: Boolean
        get() = parent == null
    val parent: Resource?
        get() = model.getParentOf(this)
    val children: Set<Resource>
        get() = model.getChildrenOf(this)

    open fun addMetric(name: String, data: MetricData): Metric {
        require(name !in _metricsByName) { "Cannot add multiple metrics with the same name" }
        val metric = MetricImpl(name, data, this)
        _metricsByName[name] = metric
        _metrics.add(metric)
        return metric
    }

}

private class RootResource(
    model: ResourceModel
) : Resource("", emptyMap(), null, model) {

    override fun addMetric(name: String, data: MetricData): Metric {
        throw IllegalArgumentException("Cannot add metrics to the root resource")
    }

}

private class SubResource(
    name: String,
    tags: Map<String, String>,
    description: String?,
    model: ResourceModel
) : Resource(name, tags, description, model)

interface Metric {
    val name: String
    val path: String
    val data: MetricData
    val resource: Resource
}

private class MetricImpl(
    override val name: String,
    override val data: MetricData,
    override val resource: Resource
) : Metric {

    override val path: String by lazy {
        "${resource.path}:$name"
    }

}