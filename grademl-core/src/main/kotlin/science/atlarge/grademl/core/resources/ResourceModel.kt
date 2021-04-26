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
        metric: Metric? = null,
        parent: Resource? = null
    ): Resource {
        val resource = Resource(name, tags, description, metric, this)
        _resources.add(resource)
        if (parent != null) addParentRelationship(parent, resource)
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
    val metric: Metric?,
    private val model: ResourceModel
) {

    val parent: Resource?
        get() = model.getParentOf(this)
    val children: Set<Resource>
        get() = model.getChildrenOf(this)

    val hasMetric: Boolean
        get() = metric != null

    fun addChild(resource: Resource) {
        model.addParentRelationship(this, resource)
    }

}