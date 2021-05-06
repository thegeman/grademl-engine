package science.atlarge.grademl.core.attribution

import science.atlarge.grademl.core.TimestampNsArray
import science.atlarge.grademl.core.execution.ExecutionPhase
import science.atlarge.grademl.core.math.NonNegativeLeastSquares
import science.atlarge.grademl.core.resources.Metric
import java.nio.file.Path

class BestFitAttributionRuleProvider(
    phases: Iterable<ExecutionPhase>,
    metrics: Iterable<Metric>,
    private val scratchDirectory: Path
) {

    private val phases = phases.toSet()
    private val orderedPhaseList = phases.sortedBy { it.path }
    private val metrics = metrics.toSet()

    private val cachedRules = mutableMapOf<Metric, Map<ExecutionPhase, ResourceAttributionRule>>()

    fun apply(phase: ExecutionPhase, metric: Metric): ResourceAttributionRule {
        // Sanity check the arguments
        if (phase !in phases || metric !in metrics) return ResourceAttributionRule.None
        // Return from cache if available
        val cachedRule = cachedRules[metric].orEmpty()[phase]
        if (cachedRule != null) {
            return cachedRule
        }
        // Otherwise, fit attribution rules to observed resource usage and phase activity
        computeFitForMetric(metric)
        return cachedRules[metric].orEmpty()[phase] ?: ResourceAttributionRule.None
    }

    private fun computeFitForMetric(metric: Metric) {
        // Create phase activity matrix and metric usage vector in preparation for NNLS fit
        val activityMatrix = createPhaseActivityMatrix(metric.data.timestamps)
        val observationVector = metric.data.values
        // Perform the NNLS fit
        val bestFit = NonNegativeLeastSquares.fit(activityMatrix, observationVector, scratchDirectory)
        // Translate the obtained coefficients to attribution rules
        cachedRules[metric] = orderedPhaseList.mapIndexed { i, phase ->
            phase to if (bestFit[i] > 0.0) {
                ResourceAttributionRule.Variable(bestFit[i])
            } else {
                ResourceAttributionRule.None
            }
        }.toMap()
    }

    private fun createPhaseActivityMatrix(
        timestamps: TimestampNsArray
    ): Array<DoubleArray> {
        return Array(timestamps.size - 1) { timePeriodId ->
            val startTime = timestamps[timePeriodId]
            val endTime = timestamps[timePeriodId + 1]
            val periodLength = endTime - startTime
            DoubleArray(phases.size) { phaseId ->
                val overlap = maxOf(
                    0,
                    minOf(endTime, orderedPhaseList[phaseId].endTime) -
                            maxOf(startTime, orderedPhaseList[phaseId].startTime)
                )
                overlap.toDouble() / periodLength
            }
        }
    }

}