package science.atlarge.grademl.cli.util

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.core.PathMatchException
import science.atlarge.grademl.core.PathMatches
import science.atlarge.grademl.core.models.execution.ExecutionPhase
import science.atlarge.grademl.core.models.execution.ExecutionPhasePath
import science.atlarge.grademl.core.models.resource.Metric
import science.atlarge.grademl.core.models.resource.MetricPath
import science.atlarge.grademl.core.models.resource.Resource
import science.atlarge.grademl.core.models.resource.ResourcePath

fun parseExecutionPhasePathExpression(pathExpression: String): ExecutionPhasePath {
    return ExecutionPhasePath.parse(pathExpression)
}

fun parseResourcePathExpression(pathExpression: String): ResourcePath {
    return ResourcePath.parse(pathExpression)
}

fun parseMetricPathExpression(pathExpression: String): MetricPath? {
    if (pathExpression.count { it == ':' } != 1) {
        println("Invalid metric path expression: \"$pathExpression\".")
        println("A metric path expression must contain exactly one ':' to separate resource path and metric name.")
        return null
    }
    val (resourcePathExpression, metricName) = pathExpression.split(':')
    return MetricPath(parseResourcePathExpression(resourcePathExpression), metricName)
}

fun tryMatchExecutionPhasePath(
    path: ExecutionPhasePath,
    cliState: CliState,
    restrictToSelected: Boolean = true
): Set<ExecutionPhase>? {
    return when (val matchResult = cliState.executionModel.resolvePath(path)) {
        is PathMatches -> {
            if (restrictToSelected) {
                val filteredMatches = matchResult.matches.intersect(cliState.phaseFilter.includedPhases)
                filteredMatches.ifEmpty {
                    println("Failed to match phase(s) for path \"$path\":")
                    println("  All matching phases are excluded from selection.")
                    null
                }
            } else {
                matchResult.matches.toSet()
            }
        }
        is PathMatchException -> {
            println("Failed to match phase(s) for path \"$path\":")
            println("  ${matchResult.message}")
            null
        }
    }
}

fun tryMatchResourcePath(path: ResourcePath, cliState: CliState, restrictToSelected: Boolean = true): Set<Resource>? {
    return when (val matchResult = cliState.resourceModel.resolvePath(path)) {
        is PathMatches -> {
            if (restrictToSelected) {
                val filteredMatches = matchResult.matches.intersect(cliState.resourceFilter.includedResources)
                filteredMatches.ifEmpty {
                    println("Failed to match resource(s) for path \"$path\":")
                    println("  All matching resources are excluded from selection.")
                    null
                }
            } else {
                matchResult.matches.toSet()
            }
        }
        is PathMatchException -> {
            println("Failed to match resource(s) for path \"$path\":")
            println("  ${matchResult.message}")
            null
        }
    }
}

fun tryMatchMetricPath(path: MetricPath, cliState: CliState, restrictToSelected: Boolean = true): Set<Metric>? {
    return when (val matchResult = cliState.resourceModel.resolvePath(path)) {
        is PathMatches -> {
            if (restrictToSelected) {
                val filteredMatches = matchResult.matches.intersect(cliState.metricFilter.includedMetrics)
                filteredMatches.ifEmpty {
                    println("Failed to match metric(s) for path \"$path\":")
                    println("  All matching metrics are excluded from selection.")
                    null
                }
            } else {
                matchResult.matches.toSet()
            }
        }
        is PathMatchException -> {
            println("Failed to match metric(s) for path \"$path\":")
            println("  ${matchResult.message}")
            null
        }
    }
}