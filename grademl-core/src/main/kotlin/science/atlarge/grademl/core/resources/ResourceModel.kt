package science.atlarge.grademl.core.resources

class ResourceModel {

    private val _resources = mutableSetOf<Resource>()

    val resources: Set<Resource>
        get() = _resources

    fun addResource(
        name: String,
        tags: Map<String, String> = emptyMap(),
        description: String? = null
    ): Resource {
        val resource = Resource(name, tags, description)
        _resources.add(resource)
        return resource
    }

}

class Resource(
    val name: String,
    val tags: Map<String, String>,
    val description: String?
)