package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.terminal.Argument
import science.atlarge.grademl.cli.terminal.ArgumentValueConstraint
import science.atlarge.grademl.cli.terminal.ParsedCommand
import science.atlarge.grademl.cli.util.parseMetricPathExpression
import science.atlarge.grademl.cli.util.tryMatchMetricPath
import science.atlarge.grademl.core.attribution.BestFitAttributionRuleProvider
import science.atlarge.grademl.core.attribution.ResourceDemandEstimationStep

object EstimateResourceDemandCommand : Command(
    name = "estimate-resource-demand",
    shortHelpMessage = "estimate resource demand for a given metric",
    longHelpMessage = "Estimate resource demand for a given metric by inferring resource attribution rules from " +
            "observed resource usage and phase activity.",
    supportedArguments = listOf(
        Argument(
            "metric_path",
            "path of metric to estimate demand for",
            ArgumentValueConstraint.MetricPath,
            isOptional = false,
            isVararg = true
        )
    )
) {

    override fun process(parsedCommand: ParsedCommand, cliState: CliState) {
        // Parse the metric_path argument value(s) for a set of metrics to estimate demand for
        val metrics = parsedCommand.getArgumentValues("metric_path")
            .map { parseMetricPathExpression(it) ?: return }
            .map { tryMatchMetricPath(it, cliState) ?: return }
            .flatten()
            .toSet()

        // Configure demand estimator to use only selected metrics and selected *leaf* phases
        val selectedLeafPhases = cliState.selectedPhases.filter { it.children.isEmpty() }.toSet()
        val demandEstimation = ResourceDemandEstimationStep(
            cliState.selectedMetrics,
            selectedLeafPhases,
            BestFitAttributionRuleProvider(
                selectedLeafPhases,
                cliState.selectedMetrics,
                cliState.scratchDirectory
            )
        )

        // Make sure the scratch directory exists
        cliState.scratchDirectory.toFile().mkdirs()

        // Estimate demand for each metric independently
        var isFirst = true
        for (metric in metrics.sortedBy { cliState.metricList.metricToIdentifier(it) }) {
            if (!isFirst) println()
            isFirst = false
            println("Estimating demand for metric: \"${metric.path}\".")
            demandEstimation.estimatedDemandForMetric(metric)
        }
    }

}