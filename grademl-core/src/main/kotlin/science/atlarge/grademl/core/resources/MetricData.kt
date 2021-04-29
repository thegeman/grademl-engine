package science.atlarge.grademl.core.resources

import science.atlarge.grademl.core.TimestampNs
import science.atlarge.grademl.core.TimestampNsArray

sealed class MetricData(val timestamps: TimestampNsArray) {
    abstract fun slice(startTime: TimestampNs, endTime: TimestampNs): MetricData
}

class LongMetricData(
    timestamps: TimestampNsArray,
    val values: LongArray,
    val maxValue: Long
) : MetricData(timestamps) {
    override fun slice(startTime: TimestampNs, endTime: TimestampNs): LongMetricData {
        var startIdx = timestamps.binarySearch(startTime)
        if (startIdx < 0) startIdx = maxOf(startIdx.inv() - 1, 0)
        var endIdx = timestamps.binarySearch(endTime)
        if (endIdx < 0) endIdx = minOf(endIdx.inv() - 1, timestamps.lastIndex)

        val selectedTimestamps = timestamps.copyOfRange(startIdx, endIdx + 1)
        val selectedValues = values.copyOfRange(startIdx, endIdx)

        return LongMetricData(selectedTimestamps, selectedValues, maxValue)
    }
}

class DoubleMetricData(
    timestamps: TimestampNsArray,
    val values: DoubleArray,
    val maxValue: Double
) : MetricData(timestamps) {
    override fun slice(startTime: TimestampNs, endTime: TimestampNs): DoubleMetricData {
        var startIdx = timestamps.binarySearch(startTime)
        if (startIdx < 0) startIdx = maxOf(startIdx.inv() - 1, 0)
        var endIdx = timestamps.binarySearch(endTime)
        if (endIdx < 0) endIdx = minOf(endIdx.inv() - 1, timestamps.lastIndex)

        val selectedTimestamps = timestamps.copyOfRange(startIdx, endIdx + 1)
        val selectedValues = values.copyOfRange(startIdx, endIdx)

        return DoubleMetricData(selectedTimestamps, selectedValues, maxValue)
    }
}