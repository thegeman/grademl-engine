package science.atlarge.grademl.core.analysis

import science.atlarge.grademl.core.FractionalDurationNs
import science.atlarge.grademl.core.FractionalDurationNsArray
import science.atlarge.grademl.core.attribution.AttributedResourceData
import science.atlarge.grademl.core.models.resource.Metric
import science.atlarge.grademl.core.models.resource.mergeTimestamps

object BottleneckIdentificationStep {

    fun analyzeMetricAttributionData(
        metricGroups: List<Set<Metric>>,
        metricAttributionData: Map<Metric, AttributedResourceData>
    ) : BottleneckIdentificationResult {
        // Sanity check metric grouping
        require(metricGroups.isNotEmpty())
        require(metricGroups.sumOf { it.size } == metricAttributionData.size)
        require(metricGroups.flatten().toSet() == metricAttributionData.keys)

        // Convenient access to number of metrics and number of metric groups
        val metricCount = metricAttributionData.size
        val metricGroupCount = metricGroups.size

        // Convert metric grouping to indexed arrays
        val metricOrder = metricAttributionData.keys.toList()
        val metricToId = metricOrder.mapIndexed { index, metric -> metric to index }.toMap()
        val groupSizes = metricGroups.map { it.size }
        val metricToGroupMap = IntArray(metricCount)
        val groupToMetricMap = IntArray(metricCount)
        val groupOffset = IntArray(metricGroupCount)
        var groupToMetricMapOffset = 0
        for (g in groupSizes.indices) {
            val metricsInGroup = metricGroups[g].toList()
            for (i in metricsInGroup.indices) {
                val metricId = metricToId[metricsInGroup[i]]!!
                metricToGroupMap[metricId] = g
                groupToMetricMap[groupToMetricMapOffset + i] = metricId
            }

            groupOffset[g] = groupToMetricMapOffset
            groupToMetricMapOffset += groupSizes[g]
        }

        // Define output variables for various kinds of simulated duration
        var durationWithoutOverhead: FractionalDurationNs = 0.0
        val durationWithoutMetricGroup = FractionalDurationNsArray(metricGroupCount)
        val durationWithoutMetricGroupAndOverhead = FractionalDurationNsArray(metricGroupCount)

        // Create iterators for individual metrics to determine their utilization over time
        val metricIterators = metricAttributionData.map { (_, attrData) ->
            attrData.metricData.iterator() to attrData.availableCapacity.iterator()
        }

        // Create scratch arrays for storing the per-metric and per-metric-group utilization data
        val metricFractionOfCapacity = DoubleArray(metricCount)
        val metricGroupFractionOfCapacity = DoubleArray(metricGroupCount)
        val shouldUpdateMetricGroup = BooleanArray(metricGroupCount)

        // Identify all unique timestamps among the metric iterators and iterator over them
        val combinedTimestamps = mergeTimestamps(metricAttributionData.map { it.value.metricData.timestamps })
        for (timePeriod in 0 until combinedTimestamps.size - 1) {
            val startTime = combinedTimestamps[timePeriod]
            val endTime = combinedTimestamps[timePeriod + 1]
            val duration = (endTime - startTime).toDouble()

            // Update metric values for this time period
            for (m in 0 until metricCount) {
                val iters = metricIterators[m]
                if (iters.first.currentEndTime <= startTime) {
                    iters.first.next()
                    iters.second.next()
                    require(iters.first.currentEndTime > startTime)
                    val newValue = iters.first.currentValue
                    val newCapacity = iters.second.currentValue
                    metricFractionOfCapacity[m] = if (newCapacity > 0.0) {
                        minOf(newValue / newCapacity, 1.0)
                    } else {
                        1.0
                    }
                    shouldUpdateMetricGroup[metricToGroupMap[m]] = true
                }
            }

            // Update metric group values (only if a metric in the group has been updated)
            for (g in 0 until metricGroupCount) {
                if (!shouldUpdateMetricGroup[g]) continue

                // Take maximum the metrics in this group
                var max = 0.0
                val offs = groupOffset[g]
                for (i in offs until offs + groupSizes[g]) {
                    val m = groupToMetricMap[i]
                    max = maxOf(max, metricFractionOfCapacity[m])
                }
                metricGroupFractionOfCapacity[g] = max
                shouldUpdateMetricGroup[g] = false
            }

            // Compute how much of this time period was spent using any metric group, assuming no overlap
            val totalMetricFraction = minOf(metricGroupFractionOfCapacity.sum(), 1.0)
            // Accumulate the total time spent using any metric group
            durationWithoutOverhead += duration * totalMetricFraction

            // Identify the most and second-most used metric group
            var highestFraction = 0.0
            var secondHighestFraction = 0.0
            for (fraction in metricGroupFractionOfCapacity) {
                if (fraction > highestFraction) {
                    secondHighestFraction = highestFraction
                    highestFraction = fraction
                } else if (fraction > secondHighestFraction) {
                    secondHighestFraction = fraction
                }
            }

            // Compute for each metric group how much time (at most) was spent using that metric group alone
            for (g in 0 until metricGroupCount) {
                val fraction = metricGroupFractionOfCapacity[g]
                val largestOtherFraction = if (highestFraction == fraction) secondHighestFraction else highestFraction
                val potentialGain = minOf(fraction, 1.0 - largestOtherFraction)

                // Accumulate the total time spent using any *other* metric group
                durationWithoutMetricGroup[g] += duration * (1.0 - potentialGain)
                durationWithoutMetricGroupAndOverhead[g] += duration * (totalMetricFraction - potentialGain)
            }
        }

        return BottleneckIdentificationResult(
            durationWithoutOverhead, durationWithoutMetricGroup, durationWithoutMetricGroupAndOverhead
        )
    }

}

class BottleneckIdentificationResult(
    val durationWithoutOverhead: FractionalDurationNs,
    val durationWithoutMetricGroup: FractionalDurationNsArray,
    val durationWithoutMetricGroupAndOverhead: FractionalDurationNsArray
)