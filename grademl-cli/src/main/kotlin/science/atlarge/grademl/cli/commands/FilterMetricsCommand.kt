package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.util.Argument
import science.atlarge.grademl.cli.util.ParsedCommand
import science.atlarge.grademl.cli.util.parseMetricPathExpression
import science.atlarge.grademl.core.PathMatchException
import science.atlarge.grademl.core.PathMatches
import science.atlarge.grademl.core.resources.Metric
import science.atlarge.grademl.core.resources.MetricPath

object FilterMetricsCommand : Command(
    name = "filter-metrics",
    shortHelpMessage = "exclude or include metrics in subsequent commands",
    longHelpMessage = "Adds or removes metrics to or from an exclusion list. " +
            "Excluded metrics do not appear in the output of most other commands, e.g., plotting.",
    supportedArguments = listOf(
        Argument(
            "exclude|include",
            "exclude or include metrics in subsequent commands"
        ),
        Argument(
            "metric_pattern",
            "regular expression matching the path of one or more metrics",
            isVararg = true
        )
    )
) {

    override fun process(parsedCommand: ParsedCommand, cliState: CliState) {
        val excludeOrInclude = parsedCommand.getArgumentValue("exclude|include")!!
        if (excludeOrInclude != "exclude" && excludeOrInclude != "include") {
            println("Invalid argument: \"$excludeOrInclude\". Expected \"exclude\" or \"include\".")
            return
        }

        val pathExpressions = parsedCommand.getArgumentValues("metric_pattern")
        val paths = mutableListOf<MetricPath>()
        for (pathExpression in pathExpressions) {
            paths.add(parseMetricPathExpression(pathExpression) ?: return)
        }

        val matchedMetrics = mutableSetOf<Metric>()
        for (path in paths) {
            when (val matchResult = cliState.resourceModel.resolvePath(path)) {
                is PathMatches -> matchedMetrics.addAll(matchResult.matches)
                is PathMatchException -> {
                    println("Failed to match metric(s) for expression \"$path\":")
                    println("  ${matchResult.message}")
                    return
                }
            }
        }

        val previousSelectionSize = cliState.selectedMetrics.size
        if (excludeOrInclude == "exclude") cliState.excludeMetrics(matchedMetrics)
        else cliState.includeMetrics(matchedMetrics)
        val newSelectionSize = cliState.selectedMetrics.size

        if (excludeOrInclude == "exclude") {
            println("Excluded ${previousSelectionSize - newSelectionSize} previously included metric(s).")
        } else {
            println("Included ${newSelectionSize - previousSelectionSize} previously excluded metric(s).")
        }
    }
}