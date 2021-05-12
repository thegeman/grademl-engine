package science.atlarge.grademl.cli.data

import science.atlarge.grademl.cli.util.JobTime
import science.atlarge.grademl.cli.util.PhaseList
import science.atlarge.grademl.core.execution.ExecutionPhase
import java.io.File

object PhaseListWriter {

    const val FILENAME = "phase-list.tsv"

    fun output(
        outFile: File,
        rootPhase: ExecutionPhase,
        selectedPhases: Iterable<ExecutionPhase>,
        phaseList: PhaseList,
        jobTime: JobTime
    ) {
        val allPhases = selectedPhases.toSet() + rootPhase
        val phaseDepths = allPhases.associateWith { countDepth(it, rootPhase) }
        require(phaseDepths.all { it.value >= 0 }) {
            "All selected phases must be a child of the provided root phase"
        }

        outFile.bufferedWriter().use { writer ->
            writer.appendLine(
                "phase.id\tphase.path\tparent.phase.id\tdepth\t" +
                        "start.time.ns\tend.time.ns\tcanonical.index"
            )
            val phasesInOrder = createCanonicalOrder(rootPhase, allPhases)
            phasesInOrder.forEachIndexed { index, phase ->
                val phaseId = phaseList.phaseToIdentifier(phase)
                val parentPhaseId = if (phase === rootPhase) {
                    phaseId
                } else {
                    phaseList.phaseToIdentifier(phase.parent!!)
                }
                val depth = phaseDepths[phase]!!
                writer.apply {
                    append(phaseId)
                    append('\t')
                    append(phase.path.toCanonicalPath().toString())
                    append('\t')
                    append(parentPhaseId)
                    append('\t')
                    append(depth.toString())
                    append('\t')
                    append(jobTime.normalize(phase.startTime).toString())
                    append('\t')
                    append(jobTime.normalize(phase.endTime).toString())
                    append('\t')
                    appendLine(index.toString())
                }
            }
        }
    }

    private fun createCanonicalOrder(
        rootPhase: ExecutionPhase,
        selectedPhases: Iterable<ExecutionPhase>
    ): List<ExecutionPhase> {
        fun collectInOrder(p: ExecutionPhase): List<ExecutionPhase> {
            val children = p.children.sortedWith(compareBy(ExecutionPhase::startTime, ExecutionPhase::identifier))
            return listOf(p) + children.flatMap(::collectInOrder)
        }

        val phaseSet = selectedPhases.toSet()
        return collectInOrder(rootPhase).filter { it in phaseSet }
    }

    private fun countDepth(phase: ExecutionPhase, rootPhase: ExecutionPhase): Int {
        var depth = 0
        var finger = phase
        while (finger != rootPhase) {
            finger = finger.parent ?: return -1
            depth++
        }
        return depth
    }

}