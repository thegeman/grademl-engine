package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.terminal.Argument
import science.atlarge.grademl.cli.terminal.ArgumentValueConstraint
import science.atlarge.grademl.cli.terminal.ParsedCommand
import science.atlarge.grademl.cli.util.parseMetricPathExpression
import science.atlarge.grademl.cli.util.tryMatchMetricPath
import science.atlarge.grademl.core.resources.MetricPath

object FilterMetricsCommand : Command(
    name = "filter-metrics",
    shortHelpMessage = "exclude or include metrics in subsequent commands",
    longHelpMessage = "Adds or removes metrics to or from an exclusion list. " +
            "Excluded metrics do not appear in the output of most other commands, e.g., plotting.",
    supportedArguments = listOf(
        Argument(
            "exclude|include",
            "exclude or include metrics in subsequent commands",
            ArgumentValueConstraint.Choice(setOf("exclude", "include"))
        ),
        Argument(
            "metric_pattern",
            "regular expression matching the path of one or more metrics",
            ArgumentValueConstraint.MetricPath,
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
        val matchedMetrics = paths.flatMap { tryMatchMetricPath(it, cliState) ?: return }.toSet()

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