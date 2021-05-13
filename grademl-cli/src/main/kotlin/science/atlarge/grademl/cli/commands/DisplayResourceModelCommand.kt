package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.terminal.Option
import science.atlarge.grademl.cli.terminal.ParsedCommand
import science.atlarge.grademl.cli.util.toDisplayString
import science.atlarge.grademl.core.models.resource.*

object DisplayResourceModelCommand : Command(
    name = "display-resource-model",
    shortHelpMessage = "display detailed information about a job's resource model",
    longHelpMessage = "Displays information about a job's resource model. By default outputs a tree of resources and metrics,\n" +
            "and for each metric a description of its time span and values.",
    supportedOptions = listOf(
        Option('s', "short", "only display resource and metric names")
    )
) {

    override fun process(parsedCommand: ParsedCommand, cliState: CliState) {
        printResourceModel(
            cliState.resourceModel,
            cliState.resourceFilter.includedResources,
            cliState.resourceFilter.allResources,
            cliState.metricFilter.includedMetrics,
            cliState.metricFilter.allMetrics,
            verbose = !parsedCommand.isOptionProvided("short")
        )
    }

    private fun printResourceModel(
        resourceModel: ResourceModel,
        selectedResources: Set<Resource>,
        allResources: Set<Resource>,
        selectedMetrics: Set<Metric>,
        allMetrics: Set<Metric>,
        verbose: Boolean
    ) {
        print("Resource model extracted from job logs")
        if (selectedResources.size != allResources.size) {
            print(" (${selectedResources.size}/${allResources.size} resources selected)")
        }
        if (selectedMetrics.size != allMetrics.size) {
            print(" (${selectedMetrics.size}/${allMetrics.size} metrics selected)")
        }
        println(":")
        if (selectedResources.isEmpty()) {
            println("  (empty)")
        } else {
            for (topLevelResource in resourceModel.rootResource.children.sortedBy { it.identifier }) {
                printResource(topLevelResource, selectedResources, selectedMetrics, "  ", verbose)
            }
        }
    }

    private fun printResource(
        resource: Resource,
        selectedResources: Set<Resource>,
        selectedMetrics: Set<Metric>,
        indent: String,
        verbose: Boolean
    ) {
        if (resource in selectedResources) {
            println("$indent/${resource.identifier}")
            for (metric in resource.metrics.sortedBy { it.name }) {
                printMetric(metric, selectedMetrics, "$indent  ", verbose)
            }
            for (childResource in resource.children.sortedBy { it.identifier }) {
                printResource(childResource, selectedResources, selectedMetrics, "$indent  ", verbose)
            }
        }
    }

    private fun printMetric(metric: Metric, selectedMetrics: Set<Metric>, indent: String, verbose: Boolean) {
        if (metric in selectedMetrics) {
            println("$indent:${metric.name}")
            if (verbose) printMetricDetails(metric.data, "$indent    ")
        }
    }

    private fun printMetricDetails(metricData: MetricData, indent: String) {
        val minTimestamp = metricData.timestamps.first().toDisplayString()
        val maxTimestamp = metricData.timestamps.last().toDisplayString()
        val valueStats = if (metricData.values.isNotEmpty()) {
            val minValue = metricData.values.minOrNull()
            val avgValue = metricData.values.average()
            val maxValue = metricData.values.maxOrNull()
            "$minValue / $avgValue / $maxValue"
        } else {
            "(none)"
        }

        println("${indent}Timestamps:            [$minTimestamp, $maxTimestamp]")
        println("${indent}Values (min/avg/max):  $valueStats")
        println("${indent}Limit value:           ${metricData.maxValue}")
    }

}