package science.atlarge.grademl.core.resources

import science.atlarge.grademl.core.TimestampNsArray

sealed class Metric(val name: String, val timestamps: TimestampNsArray)

class LongMetric(name: String, timestamps: TimestampNsArray, val values: LongArray, val maxValue: Long) :
    Metric(name, timestamps)

class DoubleMetric(name: String, timestamps: TimestampNsArray, val values: DoubleArray, val maxValue: Double) :
    Metric(name, timestamps)