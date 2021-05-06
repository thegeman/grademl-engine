package science.atlarge.grademl.core.resources

import science.atlarge.grademl.core.TimestampNs
import science.atlarge.grademl.core.TimestampNsArray

class MetricData(
    val timestamps: TimestampNsArray,
    val values: DoubleArray,
    val maxValue: Double
) {

    init {
        require(timestamps.size == values.size + 1) { "Size of timestamp and value arrays must be consistent" }
    }

    fun slice(startTime: TimestampNs, endTime: TimestampNs): MetricData {
        var startIdx = timestamps.binarySearch(startTime)
        if (startIdx < 0) startIdx = maxOf(startIdx.inv() - 1, 0)
        var endIdx = timestamps.binarySearch(endTime)
        if (endIdx < 0) endIdx = minOf(endIdx.inv() - 1, timestamps.lastIndex)

        val selectedTimestamps = timestamps.copyOfRange(startIdx, endIdx + 1)
        val selectedValues = values.copyOfRange(startIdx, endIdx)

        return MetricData(selectedTimestamps, selectedValues, maxValue)
    }
}