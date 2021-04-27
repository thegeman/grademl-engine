package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.util.Option
import science.atlarge.grademl.cli.util.ParsedCommand
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
        for (rootResource in resourceModel.rootResources.sortedBy { it.identifier }) {
            printResource(rootResource, "  ", verbose)
        }
    }

    private fun printResource(resource: Resource, indent: String, verbose: Boolean) {
        println("$indent/${resource.identifier}")
        for (metric in resource.metrics.values.sortedBy { it.name }) {
            printMetric(metric, "$indent  ", verbose)
        }
        for (childResource in resource.children.sortedBy { it.identifier }) {
            printResource(childResource, "$indent  ", verbose)
        }
    }

    private fun printMetric(metric: Metric, indent: String, verbose: Boolean) {
        println("$indent:${metric.name}")
        if (verbose) printMetricDetails(metric, "$indent    ")
    }

    private fun printMetricDetails(metric: Metric, indent: String) {
        val minTimestamp = metric.timestamps.first().let {
            "%d.%09d".format(it / 1_000_000_000, it % 1_000_000_000)
        }
        val maxTimestamp = metric.timestamps.last().let {
            "%d.%09d".format(it / 1_000_000_000, it % 1_000_000_000)
        }
        val valueStats = when (metric) {
            is DoubleMetric -> {
                if (metric.values.isNotEmpty()) {
                    val minValue = metric.values.minOrNull()
                    val avgValue = metric.values.average()
                    val maxValue = metric.values.maxOrNull()
                    "$minValue / $avgValue / $maxValue"
                } else {
                    "(none)"
                }
            }
            is LongMetric -> {
                if (metric.values.isNotEmpty()) {
                    val minValue = metric.values.minOrNull()
                    val avgValue = metric.values.average()
                    val maxValue = metric.values.maxOrNull()
                    "$minValue / $avgValue / $maxValue"
                } else {
                    "(none)"
                }
            }
        }
        val maxValue = when (metric) {
            is DoubleMetric -> metric.maxValue.toString()
            is LongMetric -> metric.maxValue.toString()
        }

        println("${indent}Timestamps:            [$minTimestamp, $maxTimestamp]")
        println("${indent}Values (min/avg/max):  $valueStats")
        println("${indent}Limit value:           $maxValue")
    }

}