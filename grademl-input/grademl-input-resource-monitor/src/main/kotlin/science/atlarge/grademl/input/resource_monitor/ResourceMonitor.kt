package science.atlarge.grademl.input.resource_monitor

import science.atlarge.grademl.core.execution.ExecutionModel
import science.atlarge.grademl.core.input.InputSource
import science.atlarge.grademl.core.resources.*
import science.atlarge.grademl.input.resource_monitor.procfs.CpuUtilizationData
import science.atlarge.grademl.input.resource_monitor.procfs.DiskUtilizationData
import science.atlarge.grademl.input.resource_monitor.procfs.NetworkUtilizationData
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.system.exitProcess

object ResourceMonitor : InputSource {

    override fun parseJobData(
        jobDataDirectories: Iterable<Path>,
        unifiedExecutionModel: ExecutionModel,
        unifiedResourceModel: ResourceModel
    ): Boolean {
        // Find Resource Monitor metric directories
        val resourceMonitorMetricDirectories = jobDataDirectories
            .map { it.resolve("metrics").resolve("resource-monitor") }
            .filter { it.toFile().isDirectory }
        if (resourceMonitorMetricDirectories.isEmpty()) return false

        // Parse Resource Monitor metrics
        val resourceMonitorMetrics = ResourceMonitorParser.parseFromDirectories(resourceMonitorMetricDirectories)

        // Add a top-level resource for the cluster
        val clusterResource = unifiedResourceModel.addResource("cluster")
        // Add a resource for each machine
        val machineResources = resourceMonitorMetrics.hostnames.associateWith { hostname ->
            unifiedResourceModel.addResource(
                name = "machine",
                tags = mapOf("hostname" to hostname),
                parent = clusterResource
            )
        }
        // Add resource-specific metrics to the resource model
        addCpuUtilizationToResourceModel(
            resourceMonitorMetrics.cpuUtilizationData,
            unifiedResourceModel,
            machineResources
        )
        addNetworkUtilizationToResourceModel(
            resourceMonitorMetrics.networkUtilizationData,
            unifiedResourceModel,
            machineResources
        )
        addDiskUtilizationToResourceModel(
            resourceMonitorMetrics.diskUtilizationData,
            unifiedResourceModel,
            machineResources
        )

        return true
    }

    private fun addCpuUtilizationToResourceModel(
        cpuUtilizationData: Map<String, CpuUtilizationData>,
        resourceModel: ResourceModel,
        machineResources: Map<String, Resource>
    ) {
        for ((hostname, cpuUtilization) in cpuUtilizationData) {
            // Create metric for total CPU utilization and add it as a resource
            val cpuResource = resourceModel.addResource(
                name = "cpu",
                parent = machineResources[hostname]!!
            )
            cpuResource.addMetric(
                name = "utilization",
                data = DoubleMetricData(
                    cpuUtilization.timestamps,
                    cpuUtilization.totalCoreUtilization,
                    cpuUtilization.numCpuCores.toDouble()
                )
            )
            cpuResource.addMetric(
                name = "cores-fully-utilized",
                data = LongMetricData(
                    cpuUtilization.timestamps,
                    cpuUtilization.coresFullyUtilized,
                    cpuUtilization.numCpuCores.toLong()
                )
            )
            // Create metrics for each individual CPU core and add them as resources
            for (core in cpuUtilization.cores) {
                val coreResource = resourceModel.addResource(
                    name = "core",
                    tags = mapOf("id" to core.coreId.toString()),
                    parent = cpuResource
                )
                coreResource.addMetric(
                    name = "utilization",
                    data = DoubleMetricData(
                        core.timestamps,
                        core.utilization,
                        1.0
                    )
                )
            }
        }
    }

    private fun addNetworkUtilizationToResourceModel(
        networkUtilizationData: Map<String, NetworkUtilizationData>,
        resourceModel: ResourceModel,
        machineResources: Map<String, Resource>
    ) {
        // Enumerate over each network interface in the monitored cluster
        for ((hostname, networkUtilization) in networkUtilizationData) {
            for ((iface, ifaceUtilization) in networkUtilization.interfaces) {
                // Create a resource for the network interface
                val ifaceResource = resourceModel.addResource(
                    name = "network",
                    tags = mapOf("iface" to iface),
                    parent = machineResources[hostname]!!
                )
                // Add metrics for incoming and outgoing traffic
                // TODO: Set maximum value for network metrics to a correct, interface-specific value
                ifaceResource.addMetric(
                    name = "bytes-received",
                    data = DoubleMetricData(
                        timestamps = ifaceUtilization.timestamps,
                        values = ifaceUtilization.bytesReceived,
                        maxValue = 1e9
                    )
                )
                ifaceResource.addMetric(
                    name = "bytes-sent", DoubleMetricData(
                        timestamps = ifaceUtilization.timestamps,
                        values = ifaceUtilization.bytesSent,
                        maxValue = 1e9
                    )
                )
            }
        }
    }

    private fun addDiskUtilizationToResourceModel(
        diskUtilizationData: Map<String, DiskUtilizationData>,
        resourceModel: ResourceModel,
        machineResources: Map<String, Resource>
    ) {
        // Enumerate over each disk device in the monitored cluster
        for ((hostname, diskUtilization) in diskUtilizationData) {
            for ((device, deviceUtilization) in diskUtilization.disks) {
                // Create a resource for the disk device
                val deviceResource = resourceModel.addResource(
                    name = "disk",
                    tags = mapOf("device" to device),
                    parent = machineResources[hostname]!!
                )
                // Add metrics for bytes read/written, read/write time, etc.
                // TODO: Set maximum value for disk metrics to a correct, device-specific value
                deviceResource.addMetric(
                    name = "bytes-read",
                    data = DoubleMetricData(
                        timestamps = deviceUtilization.timestamps,
                        values = deviceUtilization.bytesRead,
                        maxValue = 1e8
                    )
                )
                deviceResource.addMetric(
                    name = "bytes-written",
                    data = DoubleMetricData(
                        timestamps = deviceUtilization.timestamps,
                        values = deviceUtilization.bytesWritten,
                        maxValue = 1e8
                    )
                )
                deviceResource.addMetric(
                    name = "read-time",
                    data = DoubleMetricData(
                        timestamps = deviceUtilization.timestamps,
                        values = deviceUtilization.readTimeFraction,
                        maxValue = 1.0
                    )
                )
                deviceResource.addMetric(
                    name = "write-time",
                    data = DoubleMetricData(
                        timestamps = deviceUtilization.timestamps,
                        values = deviceUtilization.writeTimeFraction,
                        maxValue = 1.0
                    )
                )
                if (deviceUtilization.totalTimeSpentFraction != null) {
                    deviceResource.addMetric(
                        name = "total-utilization",
                        data = DoubleMetricData(
                            timestamps = deviceUtilization.timestamps,
                            values = deviceUtilization.totalTimeSpentFraction,
                            maxValue = 1.0
                        )
                    )
                }
            }
        }
    }

}

// Wrapper for testing the metric parser
fun main(args: Array<String>) {
    if (args.size != 1 || args[0] == "--help") {
        println("Arguments: <jobLogDirectory>")
        exitProcess(if (args.size != 1) -1 else 0)
    }

    val resourceModel = ResourceModel()
    val foundResourceMonitorMetrics = ResourceMonitor.parseJobData(
        listOf(Paths.get(args[0])),
        ExecutionModel(),
        resourceModel
    )
    require(foundResourceMonitorMetrics) {
        "Cannot find Resource Monitor logs in ${args[0]}"
    }
    println("Resource model extracted from Resource Monitor logs:")

    fun printMetric(metric: Metric, indent: String) {
        val metricData = metric.data
        val minTimestamp = metricData.timestamps.first().let {
            "%d.%09d".format(it / 1_000_000_000, it % 1_000_000_000)
        }
        val maxTimestamp = metricData.timestamps.last().let {
            "%d.%09d".format(it / 1_000_000_000, it % 1_000_000_000)
        }
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
        println("$indent:${metric.name}")
        println("$indent    Timestamps:            [$minTimestamp, $maxTimestamp]")
        println("$indent    Values (min/avg/max):  $valueStats")
        println("$indent    Limit value:           $maxValue")
    }

    fun printResource(resource: Resource, indent: String) {
        println("$indent/${resource.identifier}")
        for (metric in resource.metrics.sortedBy { it.name }) {
            printMetric(metric, "$indent  ")
        }
        for (childResource in resource.children.sortedBy { it.identifier }) {
            printResource(childResource, "$indent  ")
        }
    }
    for (topLevelResource in resourceModel.rootResource.children.sortedBy { it.identifier }) {
        printResource(topLevelResource, "  ")
    }
}