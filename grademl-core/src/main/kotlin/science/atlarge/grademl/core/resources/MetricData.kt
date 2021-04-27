package science.atlarge.grademl.core.resources

import science.atlarge.grademl.core.TimestampNsArray

sealed class MetricData(val timestamps: TimestampNsArray)

class LongMetricData(timestamps: TimestampNsArray, val values: LongArray, val maxValue: Long) :
    MetricData(timestamps)

class DoubleMetricData(timestamps: TimestampNsArray, val values: DoubleArray, val maxValue: Double) :
    MetricData(timestamps)