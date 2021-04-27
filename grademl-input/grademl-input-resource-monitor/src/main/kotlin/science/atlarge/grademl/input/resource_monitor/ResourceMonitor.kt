package science.atlarge.grademl.input.resource_monitor

import science.atlarge.grademl.core.resources.*
import science.atlarge.grademl.input.resource_monitor.procfs.CpuUtilizationData
import science.atlarge.grademl.input.resource_monitor.procfs.DiskUtilizationData
import science.atlarge.grademl.input.resource_monitor.procfs.NetworkUtilizationData
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.system.exitProcess

object ResourceMonitor {

    fun parseJobLogs(
        jobLogDirectory: Path,
        unifiedResourceModel: ResourceModel? = null
    ): ResourceModel? {
        // Parse Resource Monitor metrics
        val resourceMonitorLogDirectory = jobLogDirectory.resolve("metrics").resolve("resource-monitor")
        if (!resourceMonitorLogDirectory.toFile().isDirectory) {
            return null
        }
        val resourceMonitorMetrics = ResourceMonitorParser.parseFromDirectory(resourceMonitorLogDirectory)

        // Add a top-level resource for the cluster
        val resourceModel = unifiedResourceModel ?: ResourceModel()
        val clusterResource = resourceModel.addResource("cluster")
        // Add a resource for each machine
        val machineResources = resourceMonitorMetrics.hostnames.associateWith { hostname ->
            resourceModel.addResource(
                name = "machine",
                tags = mapOf("hostname" to hostname),
                parent = clusterResource
            )
        }
        // Add resource-specific metrics to the resource model
        addCpuUtilizationToResourceModel(resourceMonitorMetrics.cpuUtilizationData, resourceModel, machineResources)
        addNetworkUtilizationToResourceModel(
            resourceMonitorMetrics.networkUtilizationData, resourceModel,
            machineResources
        )
        addDiskUtilizationToResourceModel(resourceMonitorMetrics.diskUtilizationData, resourceModel, machineResources)

        return resourceModel
    }

    private fun addCpuUtilizationToResourceModel(
        cpuUtilizationData: Map<String, CpuUtilizationData>,
        resourceModel: ResourceModel,
        machineResources: Map<String, Resource>
    ) {
        for ((hostname, cpuUtilization) in cpuUtilizationData) {
            // Create metric for total CPU utilization and add it as a resource
            val cpuMetric = DoubleMetric(
                "utilization",
                cpuUtilization.timestamps,
                cpuUtilization.totalCoreUtilization,
                cpuUtilization.numCpuCores.toDouble()
            )
            val cpuResource = resourceModel.addResource(
                name = "cpu",
                parent = machineResources[hostname]!!
            )
            cpuResource.addMetric(cpuMetric)
            // Create metrics for each individual CPU core and add them as resources
            for (core in cpuUtilization.cores) {
                val coreMetric = DoubleMetric(
                    "utilization",
                    core.timestamps,
                    core.utilization,
                    1.0
                )
                resourceModel.addResource(
                    name = "core",
                    tags = mapOf("id" to core.coreId.toString()),
                    parent = cpuResource
                ).also {
                    it.addMetric(coreMetric)
                }
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
                // Create metrics for incoming and outgoing traffic
                // TODO: Set maximum value for network metrics to a correct, interface-specific value
                val receivedMetric = DoubleMetric(
                    name = "bytes-received",
                    timestamps = ifaceUtilization.timestamps,
                    values = ifaceUtilization.bytesReceived,
                    maxValue = 1e9
                )
                val sentMetric = DoubleMetric(
                    name = "bytes-sent",
                    timestamps = ifaceUtilization.timestamps,
                    values = ifaceUtilization.bytesSent,
                    maxValue = 1e9
                )
                // Create a resource for the network interface and add both metrics
                resourceModel.addResource(
                    name = "network",
                    tags = mapOf("iface" to iface),
                    parent = machineResources[hostname]!!
                ).also {
                    it.addMetric(receivedMetric)
                    it.addMetric(sentMetric)
                }
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
                // Create metrics for bytes read/written, read/write time, etc.
                // TODO: Set maximum value for disk metrics to a correct, device-specific value
                val metrics = mutableListOf<Metric>()
                metrics.add(
                    DoubleMetric(
                        name = "bytes-read",
                        timestamps = deviceUtilization.timestamps,
                        values = deviceUtilization.bytesRead,
                        maxValue = 1e8
                    )
                )
                metrics.add(
                    DoubleMetric(
                        name = "bytes-written",
                        timestamps = deviceUtilization.timestamps,
                        values = deviceUtilization.bytesWritten,
                        maxValue = 1e8
                    )
                )
                metrics.add(
                    DoubleMetric(
                        name = "read-time",
                        timestamps = deviceUtilization.timestamps,
                        values = deviceUtilization.readTimeFraction,
                        maxValue = 1.0
                    )
                )
                metrics.add(
                    DoubleMetric(
                        name = "write-time",
                        timestamps = deviceUtilization.timestamps,
                        values = deviceUtilization.writeTimeFraction,
                        maxValue = 1.0
                    )
                )
                if (deviceUtilization.totalTimeSpentFraction != null) {
                    metrics.add(
                        DoubleMetric(
                            name = "total-utilization",
                            timestamps = deviceUtilization.timestamps,
                            values = deviceUtilization.totalTimeSpentFraction,
                            maxValue = 1.0
                        )
                    )
                }
                // Create a resource for the disk device and add its metrics
                resourceModel.addResource(
                    name = "disk",
                    tags = mapOf("device" to device),
                    parent = machineResources[hostname]!!
                ).also {
                    for (metric in metrics) {
                        it.addMetric(metric)
                    }
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

    val resourceModel = ResourceMonitor.parseJobLogs(Paths.get(args[0]))
    requireNotNull(resourceModel) {
        "Cannot find Resource Monitor logs in ${args[0]}"
    }
    println("Resource model extracted from Resource Monitor logs:")

    fun printMetric(metric: Metric, indent: String) {
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