package science.atlarge.grademl.core.execution

import java.util.*

class ExecutionModel {

    private val phases = mutableSetOf<ExecutionPhase>()
    // Parent-child relationships
    private val phaseParents = mutableMapOf<ExecutionPhase, ExecutionPhase>()
    private val phaseChildren = mutableMapOf<ExecutionPhase, MutableSet<ExecutionPhase>>()
    // Dataflow relationships
    private val phaseOutFlows = mutableMapOf<ExecutionPhase, MutableSet<ExecutionPhase>>()

    internal fun addPhase(phase: ExecutionPhase) {
        require(phase.model === this) { "Cannot add phase from a different ExecutionModel" }
        phases.add(phase)
    }

    internal fun addParentRelationship(parentPhase: ExecutionPhase, childPhase: ExecutionPhase) {
        require(parentPhase != childPhase) { "Cannot set phase as its own parent" }
        require(parentPhase in phases && childPhase in phases) {
            "Cannot add relationship to phase(s) not part of this ExecutionModel"
        }
        require(childPhase !in phaseParents) { "Cannot add more than one parent to a phase" }
        require(!isPhaseInSubtree(parentPhase, childPhase)) { "Cannot introduce cycles in parent-child relationships" }

        phaseParents[childPhase] = parentPhase
        phaseChildren.getOrPut(parentPhase) { mutableSetOf() }.add(childPhase)
    }

    private fun isPhaseInSubtree(phase: ExecutionPhase, subtreeRoot: ExecutionPhase): Boolean {
        if (phase === subtreeRoot) return true
        val children = phaseChildren[subtreeRoot] ?: return false
        return children.any { child -> isPhaseInSubtree(phase, child) }
    }

    internal fun addDataflowRelationship(source: ExecutionPhase, sink: ExecutionPhase) {
        require(source != sink) { "Cannot add dataflow between phase and itself" }
        require(source in phases && sink in phases) {
            "Cannot add relationship to phase(s) not part of this ExecutionModel"
        }
        require(phaseParents[source] === phaseParents[sink]) {
            "Cannot add relationship to phases with different parents"
        }
        require(!dataflowPathExists(sink, source)) { "Cannot introduce cycles in dataflow relationships" }

        phaseOutFlows.getOrPut(source) { mutableSetOf() }.add(sink)
    }

    private fun dataflowPathExists(source: ExecutionPhase, sink: ExecutionPhase): Boolean {
        val phasesToCheck = LinkedList<ExecutionPhase>()
        phasesToCheck.add(source)
        while (phasesToCheck.isNotEmpty()) {
            val phase = phasesToCheck.pop()
            val nextPhases = phaseOutFlows[phase] ?: continue
            if (sink in nextPhases) return true
            phasesToCheck.addAll(nextPhases)
        }
        return false
    }

}

class ExecutionPhase(
    val name: String,
    val tags: Map<String, String> = emptyMap(),
    val description: String? = null,
    internal val model: ExecutionModel
) {

    init {
        model.addPhase(this)
    }

    fun addChild(phase: ExecutionPhase) {
        model.addParentRelationship(this, phase)
    }

    fun addOutgoingDataflow(sink: ExecutionPhase) {
        model.addDataflowRelationship(this, sink)
    }

}