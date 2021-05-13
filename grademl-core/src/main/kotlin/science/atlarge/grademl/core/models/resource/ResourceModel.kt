package science.atlarge.grademl.core.models.resource

import science.atlarge.grademl.core.*

class ResourceModel {

    // Collection of resources
    val rootResource: Resource = RootResource(this)
    private val _resources = mutableSetOf(rootResource)
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

    private val pathMatcher = PathMatcher(
        rootNode = rootResource,
        namesOfNode = { resource -> listOf(resource.name, resource.identifier) },
        parentOfNode = { resource -> getParentOf(resource) },
        childrenOfNode = { resource -> getChildrenOf(resource) }
    )

    fun resolvePath(
        path: ResourcePath,
        relativeToResource: Resource = rootResource
    ): PathMatchResult<Resource> {
        require(relativeToResource in _resources) {
            "Cannot resolve path relative to resource not in this ResourceModel"
        }
        return pathMatcher.match(path, relativeToResource)
    }

    fun resolvePath(
        path: MetricPath,
        relativeToResource: Resource = rootResource
    ): PathMatchResult<Metric> {
        require(relativeToResource in _resources) {
            "Cannot resolve path relative to resource not in this ResourceModel"
        }
        // Convert metric name to regex to support '*' in paths
        val metricNameRegex = path.metricName.split("*")
            .joinToString(separator = """.*""") { Regex.escape(it) }
            .toRegex()
        return when (val matchResult = pathMatcher.match(path.resourcePath, relativeToResource)) {
            is PathMatches -> PathMatches(matchResult.matches.flatMap { resource ->
                resource.metrics.filter { metricNameRegex.matches(it.name) }
            })
            is PathMatchException -> matchResult
        }
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

    val path: ResourcePath by lazy {
        when {
            isRoot -> ResourcePath.ROOT
            else -> parent!!.path.resolve(identifier)
        }
    }

    val isRoot: Boolean
        get() = parent == null
    val parent: Resource?
        get() = model.getParentOf(this)
    val children: Set<Resource>
        get() = model.getChildrenOf(this)

    val resourcesInTree: Set<Resource>
        get() {
            val resourceSet = mutableSetOf(this)
            for (child in children) resourceSet.addAll(child.resourcesInTree)
            return resourceSet
        }

    val metricsInTree: Set<Metric>
        get() = resourcesInTree.flatMap { it.metrics }.toSet()

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
    val path: MetricPath
    val data: MetricData
    val resource: Resource
}

private class MetricImpl(
    override val name: String,
    override val data: MetricData,
    override val resource: Resource
) : Metric {

    override val path: MetricPath = MetricPath(resource.path, name)

}

typealias ResourcePath = Path

data class MetricPath(val resourcePath: ResourcePath, val metricName: String) {
    override fun toString(): String {
        return "${resourcePath.toCanonicalPath()}:$metricName"
    }
}