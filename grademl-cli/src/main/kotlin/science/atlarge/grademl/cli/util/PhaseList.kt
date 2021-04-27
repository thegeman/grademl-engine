package science.atlarge.grademl.cli.util

import science.atlarge.grademl.core.execution.ExecutionModel
import science.atlarge.grademl.core.execution.ExecutionPhase

// Create a deterministic mapping from an execution model's phases to internally unique identifiers
class PhaseList private constructor(
    phaseOrder: List<ExecutionPhase>
) {

    private val phaseToId: Map<ExecutionPhase, String>
    private val idToPhase: Map<String, ExecutionPhase>

    init {
        val phaseCount = phaseOrder.size
        val phaseCountDigits = phaseCount.toString().length
        val phaseIdFormat = "p%0${phaseCountDigits}d"
        phaseToId = phaseOrder.withIndex().associate { it.value to phaseIdFormat.format(it.index) }
        idToPhase = phaseToId.entries.associate { it.value to it.key }
    }

    fun phaseToIdentifier(executionPhase: ExecutionPhase): String {
        require(executionPhase in phaseToId) { "Unknown phase: \"${executionPhase.path}\"" }
        return phaseToId[executionPhase]!!
    }

    fun identifierToPhase(phaseIdentifier: String): ExecutionPhase {
        require(phaseIdentifier in idToPhase) { "Unknown phase identifier: \"$phaseIdentifier\"" }
        return idToPhase[phaseIdentifier]!!
    }

    companion object {

        fun fromExecutionModel(executionModel: ExecutionModel): PhaseList {
            // Order all phases in the execution model using a DFS
            val phaseOrder = mutableListOf<ExecutionPhase>()
            appendSubtreeInOrder(executionModel.rootPhase, phaseOrder)
            return PhaseList(phaseOrder)
        }

        private fun appendSubtreeInOrder(phase: ExecutionPhase, phaseOrder: MutableList<ExecutionPhase>) {
            phaseOrder.add(phase)
            for (child in phase.children.sortedBy { it.identifier }) {
                appendSubtreeInOrder(child, phaseOrder)
            }
        }

    }

}