package science.atlarge.grademl.core.resources

import science.atlarge.grademl.core.TimestampNsArray

sealed class Metric(val timestamps: TimestampNsArray)

class LongMetric(timestamps: TimestampNsArray, val values: LongArray, val maxValue: Long) : Metric(timestamps)

class DoubleMetric(timestamps: TimestampNsArray, val values: DoubleArray, val maxValue: Double) : Metric(timestamps)