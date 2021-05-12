package science.atlarge.grademl.core.attribution

import science.atlarge.grademl.core.TimestampNsArray
import science.atlarge.grademl.core.execution.ExecutionModel
import science.atlarge.grademl.core.execution.ExecutionPhase
import science.atlarge.grademl.core.math.NonNegativeLeastSquares
import science.atlarge.grademl.core.resources.Metric
import science.atlarge.grademl.core.resources.ResourceModel
import java.nio.file.Path

class BestFitAttributionRuleProvider(
    phases: Iterable<ExecutionPhase>,
    metrics: Iterable<Metric>,
    private val scratchDirectory: Path
) : ResourceAttributionRuleProvider {

    private val phases = phases.toSet()
    private val phasesByType = phases.groupBy { it.type }
    private val orderedPhaseTypes = phasesByType.keys.sortedBy { it.path }
    private val metrics = metrics.toSet()

    private val cachedRules = mutableMapOf<Metric, Map<ExecutionPhase, ResourceAttributionRule>>()

    override fun forPhaseAndMetric(phase: ExecutionPhase, metric: Metric): ResourceAttributionRule {
        // Sanity check the arguments
        if (phase !in phases || metric !in metrics) return ResourceAttributionRule.None
        // Return from cache if available
        val cachedMetric = cachedRules[metric]
        if (cachedMetric != null) {
            return cachedMetric[phase] ?: ResourceAttributionRule.None
        }
        // Otherwise, fit attribution rules to observed resource usage and phase activity
        computeFitForMetric(metric)
        return cachedRules[metric]!![phase] ?: ResourceAttributionRule.None
    }

    private fun computeFitForMetric(metric: Metric) {
        // Create phase activity matrix and metric usage vector in preparation for NNLS fit
        val activityMatrix = createPhaseActivityMatrix(metric.data.timestamps)
        val observationVector = metric.data.values
        // Perform the NNLS fit
        val bestFit = NonNegativeLeastSquares.fit(activityMatrix, observationVector, scratchDirectory)
        // Translate the obtained coefficients to attribution rules
        val rulePerPhaseType = orderedPhaseTypes.mapIndexed { i, phaseType ->
            phaseType to if (bestFit[i] > 0.0) {
                ResourceAttributionRule.Variable(bestFit[i])
            } else {
                ResourceAttributionRule.None
            }
        }.toMap()
        cachedRules[metric] = phases.associateWith { rulePerPhaseType[it.type]!! }
    }

    private fun createPhaseActivityMatrix(
        timestamps: TimestampNsArray
    ): Array<DoubleArray> {
        return Array(timestamps.size - 1) { timePeriodId ->
            val startTime = timestamps[timePeriodId]
            val endTime = timestamps[timePeriodId + 1]
            val periodLength = endTime - startTime
            DoubleArray(orderedPhaseTypes.size) { phaseTypeId ->
                val phaseType = orderedPhaseTypes[phaseTypeId]
                phasesByType[phaseType]!!.sumOf {
                    (minOf(endTime, it.endTime) - maxOf(startTime, it.startTime)).coerceAtLeast(0)
                }.toDouble() / periodLength
            }
        }
    }

    companion object {

        fun from(
            executionModel: ExecutionModel,
            resourceModel: ResourceModel,
            outputPath: Path
        ): BestFitAttributionRuleProvider {
            return BestFitAttributionRuleProvider(
                executionModel.rootPhase.descendants.filter { it.children.isEmpty() },
                resourceModel.rootResource.metricsInTree,
                outputPath.resolve(".scratch")
            )
        }

    }

}