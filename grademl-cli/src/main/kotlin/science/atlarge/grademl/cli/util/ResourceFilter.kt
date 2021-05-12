package science.atlarge.grademl.cli.util

import science.atlarge.grademl.core.resources.Metric
import science.atlarge.grademl.core.resources.Resource
import science.atlarge.grademl.core.resources.ResourceModel
import science.atlarge.grademl.core.util.computeGraphClosure

class ResourceFilter(
    resourceModel: ResourceModel
) {

    // Inclusion list for resources
    private val _includedResources = resourceModel.resources.toMutableSet()

    // Accessors for all resources and included/excluded resources
    val allResources: Set<Resource> = _includedResources.toSet()
    val includedResources: Set<Resource>
        get() = _includedResources
    val excludedResources: Set<Resource>
        get() = allResources - includedResources

    fun includeResources(inclusions: Set<Resource>) {
        require(inclusions.all { it in allResources }) {
            "Cannot include resources that are not part of this job's resource model"
        }
        // Include all given resources and any parents
        val inclusionClosure = computeGraphClosure(inclusions) { listOfNotNull(it.parent) }
        _includedResources.addAll(inclusionClosure)
    }

    fun excludeResources(exclusions: Set<Resource>) {
        require(exclusions.all { it in allResources }) {
            "Cannot exclude resources that are not part of this job's resource model"
        }
        // Exclude all given resources and any children, except the root resource
        val exclusionClosure = computeGraphClosure(exclusions) { it.children }
        _includedResources.removeAll(exclusionClosure.filter { !it.isRoot })
    }

}