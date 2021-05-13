package science.atlarge.grademl.core.models.execution

import science.atlarge.grademl.core.TimestampNs

class PhaseEventIterator(
    phases: Iterable<ExecutionPhase>
) : Iterator<PhaseEvent> {

    private val iterator = phases.flatMap { phase ->
        listOf(PhaseStarted(phase.startTime, phase), PhaseCompleted(phase.endTime, phase))
    }.sortedWith { a, b ->
        if (a.timestamp != b.timestamp) {
            a.timestamp.compareTo(b.timestamp)
        } else {
            val aCompleted = if (a is PhaseCompleted) 1 else 0
            val bCompleted = if (b is PhaseCompleted) 1 else 0
            bCompleted - aCompleted
        }
    }.iterator()

    override fun hasNext() = iterator.hasNext()
    override fun next() = iterator.next()

}

sealed class PhaseEvent(
    val timestamp: TimestampNs,
    val phase: ExecutionPhase
)

class PhaseStarted(timestamp: TimestampNs, phase: ExecutionPhase) : PhaseEvent(timestamp, phase)

class PhaseCompleted(timestamp: TimestampNs, phase: ExecutionPhase) : PhaseEvent(timestamp, phase)

class ActivePhaseCountIterator(
    phases: Iterable<ExecutionPhase>
) : Iterator<ActivePhaseCount> {

    private val phaseEventIterator = PhaseEventIterator(phases)
    private var nextPhaseEvent: PhaseEvent? = null
    private val countObject = ActivePhaseCount(0L, 0L, 0)

    private var startTime: TimestampNs = Long.MIN_VALUE
    private var count = 0

    init {
        fetchNextEvent()
        if (nextPhaseEvent != null) {
            prepareNextPeriod()
        }
    }

    override fun hasNext() = nextPhaseEvent != null

    override fun next(): ActivePhaseCount {
        if (!hasNext()) throw NoSuchElementException()
        // Edit countObject with current period's information
        countObject.startTime = startTime
        countObject.endTime = nextPhaseEvent!!.timestamp
        countObject.count = count
        // Process the next event and any other events with the same timestamp
        prepareNextPeriod()
        return countObject
    }

    fun peekNextStartTime(): TimestampNs {
        if (!hasNext()) throw NoSuchElementException()
        return startTime
    }

    private fun fetchNextEvent() {
        nextPhaseEvent = if (phaseEventIterator.hasNext()) phaseEventIterator.next() else null
    }

    private fun prepareNextPeriod() {
        startTime = nextPhaseEvent!!.timestamp
        while (nextPhaseEvent != null && nextPhaseEvent!!.timestamp == startTime) {
            count += nextPhaseEvent!!.delta
            fetchNextEvent()
        }
    }

    companion object {
        private val PhaseEvent.delta: Int
            get() = when (this) {
                is PhaseStarted -> 1
                is PhaseCompleted -> -1
            }
    }

}

class ActivePhaseCount(var startTime: TimestampNs, var endTime: TimestampNs, var count: Int)