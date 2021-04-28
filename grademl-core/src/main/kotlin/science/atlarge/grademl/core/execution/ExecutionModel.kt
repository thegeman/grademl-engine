package science.atlarge.grademl.core.execution

import science.atlarge.grademl.core.*
import java.util.*

class ExecutionModel {

    // Collection of phases
    val rootPhase: ExecutionPhase = RootExecutionPhase(this)
    private val _phases = mutableSetOf(rootPhase)
    val phases: Set<ExecutionPhase>
        get() = _phases

    // Parent-child relationships
    private val phaseParents = mutableMapOf<ExecutionPhase, ExecutionPhase>()
    private val phaseChildren = mutableMapOf<ExecutionPhase, MutableSet<ExecutionPhase>>()

    fun getParentOf(phase: ExecutionPhase): ExecutionPhase? = phaseParents[phase]
    fun getChildrenOf(phase: ExecutionPhase): Set<ExecutionPhase> = phaseChildren[phase] ?: emptySet()

    // Dataflow relationships
    private val phaseOutFlows = mutableMapOf<ExecutionPhase, MutableSet<ExecutionPhase>>()
    private val phaseInFlows = mutableMapOf<ExecutionPhase, MutableSet<ExecutionPhase>>()

    fun getOutFlowsOf(phase: ExecutionPhase): Set<ExecutionPhase> = phaseOutFlows[phase] ?: emptySet()
    fun getInFlowsOf(phase: ExecutionPhase): Set<ExecutionPhase> = phaseInFlows[phase] ?: emptySet()

    fun addPhase(
        name: String,
        tags: Map<String, String> = emptyMap(),
        description: String? = null,
        startTime: TimestampNs,
        endTime: TimestampNs,
        parent: ExecutionPhase = rootPhase
    ): ExecutionPhase {
        require(parent in _phases) { "Cannot add phase with parent that is not part of this ExecutionModel" }
        val phase = SubExecutionPhase(name, tags, description, startTime, endTime, this)
        _phases.add(phase)
        phaseParents[phase] = parent
        phaseChildren.getOrPut(parent) { mutableSetOf() }.add(phase)
        return phase
    }

    private val pathMatcher = PathMatcher(
        rootNode = rootPhase,
        namesOfNode = { phase -> listOf(phase.name, phase.identifier) },
        parentOfNode = { phase -> getParentOf(phase) },
        childrenOfNode = { phase -> getChildrenOf(phase) }
    )

    fun resolvePath(
        path: ExecutionPhasePath,
        relativeToPhase: ExecutionPhase = rootPhase
    ): PathMatchResult<ExecutionPhase> {
        require(relativeToPhase in _phases) { "Cannot resolve path relative to phase not in this ExecutionModel" }
        return pathMatcher.match(path, relativeToPhase)
    }

    internal fun addDataflowRelationship(source: ExecutionPhase, sink: ExecutionPhase) {
        require(source != sink) { "Cannot add dataflow between phase and itself" }
        require(source in _phases && sink in _phases) {
            "Cannot add dataflow to phase(s) not part of this ExecutionModel"
        }
        require(phaseParents[source] === phaseParents[sink]) {
            "Cannot add dataflow to phases with different parents"
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

sealed class ExecutionPhase(
    private val model: ExecutionModel
) {

    abstract val name: String
    abstract val tags: Map<String, String>
    abstract val description: String?
    abstract val startTime: TimestampNs
    abstract val endTime: TimestampNs

    val identifier: String by lazy {
        if (tags.isEmpty()) name
        else "$name[${
            tags.entries.sortedBy { it.key }.joinToString(separator = ", ") { "${it.key}=${it.value}" }
        }]"
    }

    val path: ExecutionPhasePath = when {
        isRoot -> ExecutionPhasePath.ROOT
        else -> parent!!.path.resolve(identifier)
    }

    val duration: DurationNs
        get() = if (startTime < endTime) endTime - startTime else 0

    val isRoot: Boolean
        get() = parent == null
    val parent: ExecutionPhase?
        get() = model.getParentOf(this)
    val children: Set<ExecutionPhase>
        get() = model.getChildrenOf(this)
    val outFlows: Set<ExecutionPhase>
        get() = model.getOutFlowsOf(this)
    val inFlows: Set<ExecutionPhase>
        get() = model.getInFlowsOf(this)

    fun addOutgoingDataflow(sink: ExecutionPhase) {
        model.addDataflowRelationship(this, sink)
    }

}

private class RootExecutionPhase(
    model: ExecutionModel
) : ExecutionPhase(model) {
    override val name: String
        get() = ""
    override val tags: Map<String, String>
        get() = emptyMap()
    override val description: String?
        get() = null
    override val startTime: TimestampNs
        get() = children.map { it.startTime }.minOrNull() ?: 0L
    override val endTime: TimestampNs
        get() = children.map { it.endTime }.maxOrNull() ?: 0L
}

private class SubExecutionPhase(
    override val name: String,
    override val tags: Map<String, String>,
    override val description: String?,
    override val startTime: TimestampNs,
    override val endTime: TimestampNs,
    model: ExecutionModel
) : ExecutionPhase(model)

typealias ExecutionPhasePath = Path