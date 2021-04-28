package science.atlarge.grademl.cli.util

import science.atlarge.grademl.core.execution.ExecutionPhasePath
import science.atlarge.grademl.core.resources.MetricPath
import science.atlarge.grademl.core.resources.ResourcePath

fun parseExecutionPhasePathExpression(pathExpression: String): ExecutionPhasePath {
    return ExecutionPhasePath.parse(pathExpression)
}

fun parseResourcePathExpression(pathExpression: String): ResourcePath {
    return ResourcePath.parse(pathExpression)
}

fun parseMetricPathExpression(pathExpression: String): MetricPath? {
    if (pathExpression.count {  it == ':' } != 1) {
        println("Invalid metric path expression: \"$pathExpression\".")
        println("A metric path expression must contain exactly one ':' to separate resource path and metric name.")
        return null
    }
    val (resourcePathExpression, metricName) = pathExpression.split(':')
    return MetricPath(parseResourcePathExpression(resourcePathExpression), metricName)
}