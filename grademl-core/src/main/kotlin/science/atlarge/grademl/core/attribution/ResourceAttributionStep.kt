package science.atlarge.grademl.core.attribution

import science.atlarge.grademl.core.TimestampNs
import science.atlarge.grademl.core.models.execution.ExecutionPhase
import science.atlarge.grademl.core.models.resource.Metric
import science.atlarge.grademl.core.models.resource.MetricData
import science.atlarge.grademl.core.util.DoubleArrayBuilder
import science.atlarge.grademl.core.util.LongArrayBuilder

class ResourceAttributionStep(
    private val phases: Set<ExecutionPhase>,
    private val metrics: Set<Metric>,
    private val attributionRuleProvider: ResourceAttributionRuleProvider,
    private val resourceDemandEstimates: (Metric) -> ResourceDemandEstimate,
    private val upsampledMetricData: (Metric) -> MetricData
) {

    private val cachedAttributedUsage = mutableMapOf<ExecutionPhase, MutableMap<Metric, MetricData>>()

    fun attributeMetricToPhase(metric: Metric, phase: ExecutionPhase): MetricData? {
        // Return cached outcome if it exists
        cachedAttributedUsage[phase]?.get(metric)?.let { return it }
        // Otherwise, check arguments for validity
        if (metric !in metrics) return null
        if (phase !in phases) return null
        // Perform attribution step
        val newAttributedUsage = computeAttributedUsage(metric, phase)
        cachedAttributedUsage.getOrPut(phase) { mutableMapOf() }[metric] = newAttributedUsage
        return newAttributedUsage
    }

    private fun computeAttributedUsage(metric: Metric, phase: ExecutionPhase): MetricData {
        // Return an empty metric if the phase has no duration
        if (phase.duration == 0L) {
            return MetricData(longArrayOf(phase.startTime), doubleArrayOf(), metric.data.maxValue)
        }
        // Get attribution rule for phase to determine how to attribute resource usage to the phase
        val attributionRule = attributionRuleProvider.forPhaseAndMetric(phase, metric) ?: ResourceAttributionRule.None
        // Return a flat attributed value of zero if the phase does not use the given resource
        if (attributionRule is ResourceAttributionRule.None) {
            return MetricData(
                longArrayOf(phase.startTime, phase.endTime),
                doubleArrayOf(0.0),
                metric.data.maxValue
            )
        }
        // Determine the phase's demand for the resource
        val phaseDemand = when (attributionRule) {
            is ResourceAttributionRule.Exact -> attributionRule.demand
            is ResourceAttributionRule.Variable -> attributionRule.demand
            else -> throw IllegalStateException()
        }
        val isExactDemand = attributionRule is ResourceAttributionRule.Exact
        // Get output of previous steps in the attribution process
        val upsampledMetric = upsampledMetricData(metric)
        val estimatedDemand = resourceDemandEstimates(metric)
        // Perform the resource attribution step
        return ResourceAttributionComputation(
            upsampledMetric, estimatedDemand.exactDemandOverTime, estimatedDemand.variableDemandOverTime,
            phaseDemand, isExactDemand, phase.startTime, phase.endTime
        ).getAttributedUsage()
    }

}

private class ResourceAttributionComputation(
    upsampledMetric: MetricData,
    totalExactDemand: MetricData,
    totalVariableDemand: MetricData,
    private val phaseDemand: Double,
    private val isPhaseDemandExact: Boolean,
    private val startTime: TimestampNs,
    private val endTime: TimestampNs
) {

    private val metricIterator = upsampledMetric.iteratorFrom(startTime)
    private val exactDemandIterator = totalExactDemand.iteratorFrom(startTime)
    private val variableDemandIterator = totalVariableDemand.iteratorFrom(startTime)

    private var nextMetricTime = metricIterator.currentEndTime
    private var nextExactTime = exactDemandIterator.currentEndTime
    private var nextVariableTime = variableDemandIterator.currentEndTime

    private val timestamps = LongArrayBuilder()
    private val values = DoubleArrayBuilder()
    private val maxValue = if (isPhaseDemandExact) phaseDemand else upsampledMetric.maxValue

    fun getAttributedUsage(): MetricData {
        if (timestamps.size == 0) {
            attributeResource()
        }
        return MetricData(timestamps.toArray(), values.toArray(), maxValue)
    }

    private fun attributeResource() {
        // Record the start of the first measurement period
        timestamps.append(startTime)
        // Iterate over change points in demand and metric value until the given end time of the phase
        var lastTime = startTime
        while (lastTime < endTime) {
            // Find the end of the current time period
            val newTime = minOf(endTime, nextMetricTime, nextExactTime, nextVariableTime)
            // Attribute metric usage for the current time period
            val newValue = if (isPhaseDemandExact) {
                minOf(metricIterator.currentValue * phaseDemand / exactDemandIterator.currentValue, phaseDemand)
            } else {
                val leftOver = maxOf(metricIterator.currentValue - exactDemandIterator.currentValue, 0.0)
                leftOver * phaseDemand / variableDemandIterator.currentValue
            }
            // Create a data point
            emitDataPoint(newTime, newValue)
            // Move to the next time period
            if (metricIterator.currentEndTime == newTime) {
                metricIterator.next()
                nextMetricTime = metricIterator.currentEndTime
            }
            if (exactDemandIterator.currentEndTime == newTime) {
                exactDemandIterator.next()
                nextExactTime = exactDemandIterator.currentEndTime
            }
            if (variableDemandIterator.currentEndTime == newTime) {
                variableDemandIterator.next()
                nextVariableTime = variableDemandIterator.currentEndTime
            }
            lastTime = newTime
        }
    }

    private fun emitDataPoint(timestamp: TimestampNs, value: Double) {
        if (values.size > 0 && value == values.last()) {
            timestamps.replaceLast(timestamp)
        } else {
            timestamps.append(timestamp)
            values.append(value)
        }
    }

}