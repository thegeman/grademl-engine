package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.terminal.Argument
import science.atlarge.grademl.cli.terminal.ArgumentValueConstraint
import science.atlarge.grademl.cli.terminal.ParsedCommand
import science.atlarge.grademl.cli.util.parseExecutionPhasePathExpression
import science.atlarge.grademl.cli.util.tryMatchExecutionPhasePath
import science.atlarge.grademl.core.models.execution.ExecutionPhase

object IdentifyBottlenecksCommand : Command(
    name = "identify-bottlenecks",
    shortHelpMessage = "identify bottlenecks of a particular phase",
    longHelpMessage = "Identify bottlenecks of a particular phase and display the impact of various metrics" +
            " and resources on the phase's duration.",
    supportedArguments = listOf(
        Argument(
            "phase_path",
            "path of phase to identify bottlenecks for",
            ArgumentValueConstraint.MetricPath,
            isOptional = false,
            isVararg = true
        )
    )
) {

    override fun process(parsedCommand: ParsedCommand, cliState: CliState) {
        // Parse the phase_path argument value(s) for a set of phases to analyze
        val phases = parsedCommand.getArgumentValues("phase_path")
            .map { parseExecutionPhasePathExpression(it) }
            .map { tryMatchExecutionPhasePath(it, cliState) ?: return }
            .flatten()
            .toSet()

        // For each phase, identify and display bottlenecks
        var isFirst = true
        for (phase in phases) {
            if (!isFirst) println()
            isFirst = false
            println("Identifying bottlenecks for phase: \"${phase.path}\".")
            identifyBottlenecksInPhase(phase, cliState)
        }
    }

    private fun identifyBottlenecksInPhase(phase: ExecutionPhase, cliState: CliState) {
        val bottlenecks = cliState.bottleneckIdentification.analyzePhase(phase, cliState.metricFilter.includedMetrics)
        println("Bottleneck report for phase: \"${phase.path}\"")
        println("  Phase duration: ${bottlenecks.observedDuration} ns")
        println("  Estimated overhead: ${bottlenecks.observedDuration - bottlenecks.durationWithoutOverhead} ns")
        println("  Phase duration without overhead: ${bottlenecks.durationWithoutOverhead} ns")
        println("  Top resource type bottlenecks:")
        for ((resourceType, duration) in bottlenecks.durationWithoutResourceType.entries.sortedBy { it.value }) {
            val impact = bottlenecks.observedDuration - duration
            println("    $impact ns (${"%.2f".format(100 * impact / bottlenecks.observedDuration)}%-" +
                    "${"%.2f".format(100 * impact / bottlenecks.durationWithoutOverhead)}%) -- $resourceType")
        }
//        println("  Top resource bottlenecks:")
//        for ((resource, duration) in bottlenecks.durationWithoutResource.entries.sortedBy { it.value }) {
//            val impact = bottlenecks.observedDuration - duration
//            println("    $impact ns (${"%.2f".format(100 * impact / bottlenecks.observedDuration)}%) -- ${resource.path}")
//        }
        println("  Top metric type bottlenecks:")
        for ((metricType, duration) in bottlenecks.durationWithoutMetricType.entries.sortedBy { it.value }) {
            val impact = bottlenecks.observedDuration - duration
            println("    $impact ns (${"%.2f".format(100 * impact / bottlenecks.observedDuration)}%-" +
                    "${"%.2f".format(100 * impact / bottlenecks.durationWithoutOverhead)}%) -- $metricType")
        }
//        println("  Top metric bottlenecks:")
//        for ((metric, duration) in bottlenecks.durationWithoutMetric.entries.sortedBy { it.value }) {
//            val impact = bottlenecks.observedDuration - duration
//            println("    $impact ns (${"%.2f".format(100 * impact / bottlenecks.observedDuration)}%) -- ${metric.path}")
//        }
    }

}