package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.terminal.Argument
import science.atlarge.grademl.cli.terminal.ArgumentValueConstraint
import science.atlarge.grademl.cli.terminal.ParsedCommand
import science.atlarge.grademl.cli.util.parseMetricPathExpression
import science.atlarge.grademl.cli.util.tryMatchMetricPath
import science.atlarge.grademl.core.attribution.BestFitAttributionRuleProvider
import science.atlarge.grademl.core.attribution.ResourceAttributionStep
import science.atlarge.grademl.core.attribution.ResourceDemandEstimationStep
import science.atlarge.grademl.core.attribution.ResourceUpsamplingStep

object AttributeResourceCommand : Command(
    name = "attribute-resource",
    shortHelpMessage = "attribute resource usage to execution phases for a given metric",
    longHelpMessage = "Attributed observed resource usage for a given metric to execution phase using GradeML's " +
            "resource demand estimation, resource upsampling, and resource attribution steps.",
    supportedArguments = listOf(
        Argument(
            "metric_path",
            "path of metric to attribute to phases",
            ArgumentValueConstraint.MetricPath,
            isOptional = false,
            isVararg = true
        )
    )
) {

    override fun process(parsedCommand: ParsedCommand, cliState: CliState) {
        // Parse the metric_path argument value(s) for a set of metrics to attribute to phases
        val metrics = parsedCommand.getArgumentValues("metric_path")
            .map { parseMetricPathExpression(it) ?: return }
            .map { tryMatchMetricPath(it, cliState) ?: return }
            .flatten()
            .toSet()

        // Configure attribution rule provider to use only selected metrics and selected *leaf* phases
        val selectedLeafPhases = cliState.selectedPhases.filter { it.children.isEmpty() }.toSet()
        val attributionRuleProvider = BestFitAttributionRuleProvider(
            selectedLeafPhases,
            cliState.selectedMetrics,
            cliState.scratchDirectory
        )

        // Configure demand estimator
        val demandEstimationStep = ResourceDemandEstimationStep(
            cliState.selectedMetrics,
            selectedLeafPhases,
            attributionRuleProvider
        )

        // Configure resource upsampling step
        val upsamplingStep = ResourceUpsamplingStep(cliState.selectedMetrics) { m ->
            demandEstimationStep.estimatedDemandForMetric(m)!!
        }

        // Configure resource attribution step
        val attributionStep = ResourceAttributionStep(
            selectedLeafPhases,
            cliState.selectedMetrics,
            attributionRuleProvider,
            { m -> demandEstimationStep.estimatedDemandForMetric(m)!! },
            { m -> upsamplingStep.upsampleMetric(m)!! }
        )

        // Perform resource attribution for each (phase, metric) pair
        for (metric in metrics) {
            for (phase in selectedLeafPhases) {
                println("Attributing metric \"${metric.path}\" to phase \"${phase.path}\".")
                attributionStep.attributeMetricToPhase(metric, phase)
            }
        }
    }

}