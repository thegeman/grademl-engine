package science.atlarge.grademl.cli.util

import science.atlarge.grademl.core.TimestampNs
import science.atlarge.grademl.core.execution.ExecutionModel

class JobTime(
    executionModel: ExecutionModel
) {

    private val earliestTimestamp = executionModel.rootPhase.startTime

    fun normalize(plainTimestamp: TimestampNs): Long = plainTimestamp - earliestTimestamp
    fun denormalize(normalizedTimestamp: Long): TimestampNs = normalizedTimestamp + earliestTimestamp

}