package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.util.Option
import science.atlarge.grademl.cli.util.ParsedCommand
import science.atlarge.grademl.cli.util.toDisplayString
import science.atlarge.grademl.core.resources.*

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
            verbose = !parsedCommand.isOptionProvided("short")
        )
    }

    private fun printResourceModel(resourceModel: ResourceModel, verbose: Boolean) {
        println("Resource model extracted from job logs:")
        for (topLevelResource in resourceModel.rootResource.children.sortedBy { it.identifier }) {
            printResource(topLevelResource, "  ", verbose)
        }
    }

    private fun printResource(resource: Resource, indent: String, verbose: Boolean) {
        println("$indent/${resource.identifier}")
        for (metric in resource.metrics.sortedBy { it.name }) {
            printMetric(metric, "$indent  ", verbose)
        }
        for (childResource in resource.children.sortedBy { it.identifier }) {
            printResource(childResource, "$indent  ", verbose)
        }
    }

    private fun printMetric(metric: Metric, indent: String, verbose: Boolean) {
        println("$indent:${metric.name}")
        if (verbose) printMetricDetails(metric.data, "$indent    ")
    }

    private fun printMetricDetails(metricData: MetricData, indent: String) {
        val minTimestamp = metricData.timestamps.first().toDisplayString()
        val maxTimestamp = metricData.timestamps.last().toDisplayString()
        val valueStats = when (metricData) {
            is DoubleMetricData -> {
                if (metricData.values.isNotEmpty()) {
                    val minValue = metricData.values.minOrNull()
                    val avgValue = metricData.values.average()
                    val maxValue = metricData.values.maxOrNull()
                    "$minValue / $avgValue / $maxValue"
                } else {
                    "(none)"
                }
            }
            is LongMetricData -> {
                if (metricData.values.isNotEmpty()) {
                    val minValue = metricData.values.minOrNull()
                    val avgValue = metricData.values.average()
                    val maxValue = metricData.values.maxOrNull()
                    "$minValue / $avgValue / $maxValue"
                } else {
                    "(none)"
                }
            }
        }
        val maxValue = when (metricData) {
            is DoubleMetricData -> metricData.maxValue.toString()
            is LongMetricData -> metricData.maxValue.toString()
        }

        println("${indent}Timestamps:            [$minTimestamp, $maxTimestamp]")
        println("${indent}Values (min/avg/max):  $valueStats")
        println("${indent}Limit value:           $maxValue")
    }

}