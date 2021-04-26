package science.atlarge.grademl.core.resources

class ResourceModel {

    private val _resources = mutableSetOf<Resource>()

    // Parent-child relationships
    private val resourceParents = mutableMapOf<Resource, Resource>()
    private val resourceChildren = mutableMapOf<Resource, MutableSet<Resource>>()

    val resources: Set<Resource>
        get() = _resources

    fun getParentOf(resource: Resource): Resource? = resourceParents[resource]
    fun getChildrenOf(resource: Resource): Set<Resource> = resourceChildren[resource] ?: emptySet()

    fun addResource(
        name: String,
        tags: Map<String, String> = emptyMap(),
        description: String? = null,
        metrics: Iterable<Metric> = emptyList(),
        parent: Resource? = null
    ): Resource {
        val resource = Resource(name, tags, description, this)
        _resources.add(resource)
        if (parent != null) addParentRelationship(parent, resource)
        for (metric in metrics) resource.addMetric(metric)
        return resource
    }

    internal fun addParentRelationship(parentResource: Resource, childResource: Resource) {
        require(parentResource != childResource) { "Cannot set resource as its own parent" }
        require(parentResource in _resources && childResource in _resources) {
            "Cannot add relationship to resource(s) not part of this ResourceModel"
        }
        require(childResource !in resourceParents) { "Cannot add more than one parent to a resource" }
        require(!isResourceInSubtree(parentResource, childResource)) {
            "Cannot introduce cycles in parent-child relationships"
        }

        resourceParents[childResource] = parentResource
        resourceChildren.getOrPut(parentResource) { mutableSetOf() }.add(childResource)
    }

    private fun isResourceInSubtree(resource: Resource, subtreeRoot: Resource): Boolean {
        if (resource === subtreeRoot) return true
        val children = resourceChildren[subtreeRoot] ?: return false
        return children.any { child -> isResourceInSubtree(resource, child) }
    }

}

class Resource(
    val name: String,
    val tags: Map<String, String>,
    val description: String?,
    private val model: ResourceModel
) {

    private val _metrics = mutableMapOf<String, Metric>()
    val metrics: Map<String, Metric>
        get() = _metrics

    val parent: Resource?
        get() = model.getParentOf(this)
    val children: Set<Resource>
        get() = model.getChildrenOf(this)

    fun addChild(resource: Resource) {
        model.addParentRelationship(this, resource)
    }

    fun addMetric(metric: Metric) {
        require(metric.name !in _metrics) { "Cannot add multiple metrics with the same name" }
        _metrics[metric.name] = metric
    }

}