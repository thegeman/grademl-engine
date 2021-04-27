package science.atlarge.grademl.cli.util

import science.atlarge.grademl.core.TimestampNs

fun TimestampNs.toDisplayString(): String {
    return "%d.%09d".format(this / 1_000_000_000, this % 1_000_000_000)
}