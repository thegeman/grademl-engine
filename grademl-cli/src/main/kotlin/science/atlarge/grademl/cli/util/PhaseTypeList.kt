package science.atlarge.grademl.cli.util

import science.atlarge.grademl.core.execution.ExecutionModel
import science.atlarge.grademl.core.execution.ExecutionPhase
import science.atlarge.grademl.core.execution.ExecutionPhaseType

// Create a deterministic mapping from an execution model's phase types to internally unique identifiers
class PhaseTypeList private constructor(
    phaseTypeOrder: List<ExecutionPhaseType>
) {

    private val phaseTypeToId: Map<ExecutionPhaseType, String>
    private val idToPhaseType: Map<String, ExecutionPhaseType>

    init {
        val phaseTypeCount = phaseTypeOrder.size
        val phaseTypeCountDigits = phaseTypeCount.toString().length
        val phaseTypeIdFormat = "pt%0${phaseTypeCountDigits}d"
        phaseTypeToId = phaseTypeOrder.withIndex().associate { it.value to phaseTypeIdFormat.format(it.index) }
        idToPhaseType = phaseTypeToId.entries.associate { it.value to it.key }
    }

    fun phaseTypeToIdentifier(executionPhaseType: ExecutionPhaseType): String {
        require(executionPhaseType in phaseTypeToId) { "Unknown phase type: \"${executionPhaseType.path}\"" }
        return phaseTypeToId[executionPhaseType]!!
    }

    fun identifierToPhaseType(phaseTypeIdentifier: String): ExecutionPhaseType {
        require(phaseTypeIdentifier in idToPhaseType) { "Unknown phase type identifier: \"$phaseTypeIdentifier\"" }
        return idToPhaseType[phaseTypeIdentifier]!!
    }

    companion object {

        fun fromExecutionModel(executionModel: ExecutionModel): PhaseTypeList {
            // Order all phase types in the execution model using a DFS
            val phasesPerType = executionModel.phases.groupBy { it.type }
            val phaseTypeOrder = mutableListOf<ExecutionPhaseType>()
            appendSubtreeInOrder(executionModel.rootPhase.type, listOf(executionModel.rootPhase), phaseTypeOrder)
            return PhaseTypeList(phaseTypeOrder)
        }

        private fun appendSubtreeInOrder(
            phaseType: ExecutionPhaseType,
            phasesWithType: Iterable<ExecutionPhase>,
            phaseTypeOrder: MutableList<ExecutionPhaseType>
        ) {
            phaseTypeOrder.add(phaseType)
            val childTypes = phasesWithType.flatMap { it.children }.groupBy { it.type }
            for (childType in childTypes.keys.sortedBy { it.path.pathComponents.last() }) {
                val childPhasesWithType = childTypes[childType]!!
                appendSubtreeInOrder(childType, childPhasesWithType, phaseTypeOrder)
            }
        }

    }

}