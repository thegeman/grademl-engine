package science.atlarge.grademl.core.execution

class ExecutionModel {

    private val phases = mutableSetOf<ExecutionPhase>()

    internal fun addPhase(phase: ExecutionPhase) {
        phases.add(phase)
    }

}

class ExecutionPhase(
    val name: String,
    val tags: Map<String, String> = emptyMap(),
    val description: String? = null,
    private val model: ExecutionModel
) {

    init {
        model.addPhase(this)
    }

}