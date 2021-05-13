package science.atlarge.grademl.cli.data

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.core.models.execution.ExecutionPhaseType
import java.io.File

object PhaseTypeListWriter {

    const val FILENAME = "phase-type-list.tsv"

    fun output(
        outFile: File,
        selectedPhaseTypes: Iterable<ExecutionPhaseType>,
        cliState: CliState
    ) {
        // Compute the depth of every selected phase type
        val phaseTypeSet = selectedPhaseTypes.toSet()
        val phaseTypeDepths = phaseTypeSet.associateWith { it.path.pathComponents.size }

        // Create a canonical order of phase types based on first occurrence
        val phaseTypesInOrder = phaseTypeSet.map { phaseType ->
            phaseType to cliState.executionModel.phases.filter { it.type == phaseType }.minOf { it.startTime }
        }.sortedBy { it.second }.map { it.first }

        outFile.bufferedWriter().use { writer ->
            writer.appendLine(
                "phase.type.id\tphase.type.path\tparent.phase.type.id\tdepth\tcanonical.index"
            )
            phaseTypesInOrder.forEachIndexed { index, phaseType ->
                val phaseTypeId = cliState.phaseTypeList.phaseTypeToIdentifier(phaseType)
                val parentPhaseTypeId = phaseType.parent?.let { parentType ->
                    cliState.phaseTypeList.phaseTypeToIdentifier(parentType)
                } ?: "-"
                val depth = phaseTypeDepths[phaseType]!!
                writer.apply {
                    append(phaseTypeId)
                    append('\t')
                    append(phaseType.path.toString())
                    append('\t')
                    append(parentPhaseTypeId)
                    append('\t')
                    append(depth.toString())
                    append('\t')
                    appendLine(index.toString())
                }
            }
        }
    }

}