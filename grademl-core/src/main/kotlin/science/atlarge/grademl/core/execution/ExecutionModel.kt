package science.atlarge.grademl.core.execution

import java.util.*

class ExecutionModel {

    private val _phases = mutableSetOf<ExecutionPhase>()
    // Parent-child relationships
    private val phaseParents = mutableMapOf<ExecutionPhase, ExecutionPhase>()
    private val phaseChildren = mutableMapOf<ExecutionPhase, MutableSet<ExecutionPhase>>()
    // Dataflow relationships
    private val phaseOutFlows = mutableMapOf<ExecutionPhase, MutableSet<ExecutionPhase>>()
    private val phaseInFlows = mutableMapOf<ExecutionPhase, MutableSet<ExecutionPhase>>()

    val phases: Set<ExecutionPhase>
        get() = _phases
    val rootPhases: Set<ExecutionPhase>
        get() = _phases - phaseParents.keys

    fun getParentOf(phase: ExecutionPhase): ExecutionPhase? = phaseParents[phase]
    fun getChildrenOf(phase: ExecutionPhase): Set<ExecutionPhase> = phaseChildren[phase] ?: emptySet()
    fun getOutFlowsOf(phase: ExecutionPhase): Set<ExecutionPhase> = phaseOutFlows[phase] ?: emptySet()
    fun getInFlowsOf(phase: ExecutionPhase): Set<ExecutionPhase> = phaseInFlows[phase] ?: emptySet()

    fun addPhase(
        name: String,
        tags: Map<String, String> = emptyMap(),
        description: String? = null
    ): ExecutionPhase {
        val phase = ExecutionPhase(name, tags, description, this)
        _phases.add(phase)
        return phase
    }

    internal fun addParentRelationship(parentPhase: ExecutionPhase, childPhase: ExecutionPhase) {
        require(parentPhase != childPhase) { "Cannot set phase as its own parent" }
        require(parentPhase in _phases && childPhase in _phases) {
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
        require(source in _phases && sink in _phases) {
            "Cannot add relationship to phase(s) not part of this ExecutionModel"
        }
        require(phaseParents[source] === phaseParents[sink]) {
            "Cannot add relationship to phases with different parents"
        }
        require(!dataflowPathExists(sink, source)) { "Cannot introduce cycles in dataflow relationships" }

        phaseOutFlows.getOrPut(source) { mutableSetOf() }.add(sink)
        phaseInFlows.getOrPut(sink) { mutableSetOf() }.add(source)
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

class ExecutionPhase internal constructor(
    val name: String,
    val tags: Map<String, String> = emptyMap(),
    val description: String? = null,
    internal val model: ExecutionModel
) {

    val identifier = if (tags.isEmpty()) {
        name
    } else {
        "$name[${tags.entries.sortedBy { it.key }.joinToString(separator = ", ") { "${it.key}=${it.value}" }}]"
    }

    val parent: ExecutionPhase?
        get() = model.getParentOf(this)
    val children: Set<ExecutionPhase>
        get() = model.getChildrenOf(this)
    val outFlows: Set<ExecutionPhase>
        get() = model.getOutFlowsOf(this)
    val inFlows: Set<ExecutionPhase>
        get() = model.getInFlowsOf(this)

    fun addChild(phase: ExecutionPhase) {
        model.addParentRelationship(this, phase)
    }

    fun addOutgoingDataflow(sink: ExecutionPhase) {
        model.addDataflowRelationship(this, sink)
    }

}