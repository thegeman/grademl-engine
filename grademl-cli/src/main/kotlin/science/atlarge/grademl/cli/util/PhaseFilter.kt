package science.atlarge.grademl.cli.util

import science.atlarge.grademl.core.models.execution.ExecutionModel
import science.atlarge.grademl.core.models.execution.ExecutionPhase
import science.atlarge.grademl.core.util.computeGraphClosure

class PhaseFilter(
    executionModel: ExecutionModel
) {

    // Inclusion list for phases
    private val _includedPhases = executionModel.phases.toMutableSet()

    // Accessors for all phases and included/excluded phases
    val allPhases: Set<ExecutionPhase> = _includedPhases.toSet()
    val includedPhases: Set<ExecutionPhase>
        get() = _includedPhases
    val excludedPhases: Set<ExecutionPhase>
        get() = allPhases - includedPhases

    fun includePhases(inclusions: Set<ExecutionPhase>) {
        require(inclusions.all { it in allPhases }) {
            "Cannot include phases that are not part of this job's execution model"
        }
        // Include all given phases and any parents
        val inclusionClosure = computeGraphClosure(inclusions) { listOfNotNull(it.parent) }
        _includedPhases.addAll(inclusionClosure)
    }

    fun excludePhases(exclusions: Set<ExecutionPhase>) {
        require(exclusions.all { it in allPhases }) {
            "Cannot exclude phases that are not part of this job's execution model"
        }
        // Exclude all given phases and any children, except the root phase
        val exclusionClosure = computeGraphClosure(exclusions) { it.children }
        _includedPhases.removeAll(exclusionClosure.filter { !it.isRoot })
    }

}