package science.atlarge.grademl.input.resource_monitor

import science.atlarge.grademl.input.resource_monitor.procfs.*
import java.nio.file.Path

class ResourceMonitorParser private constructor(
    private val resourceMonitorMetricDirectory: Path
) {

    private val hostnames = mutableSetOf<String>()
    private val cpuUtilizationData = mutableMapOf<String, CpuUtilizationData>()
    private val networkUtilizationData = mutableMapOf<String, NetworkUtilizationData>()
    private val diskUtilizationData = mutableMapOf<String, DiskUtilizationData>()

    fun parse(): ResourceMonitorMetrics {
        findHostnames()
        parseCpuUtilization()
        parseNetworkUtilization()
        parseDiskUtilization()
        return ResourceMonitorMetrics(hostnames, cpuUtilizationData, networkUtilizationData, diskUtilizationData)
    }

    private fun findHostnames() {
        // Enumerate metric files and extract hostnames from the filenames
        resourceMonitorMetricDirectory.toFile()
            .walk()
            .filter { it.isFile && "-" in it.name }
            .map { it.name.split("-").last() }
            .toCollection(hostnames)
    }

    private fun parseCpuUtilization() {
        // Find relevant CPU metric files
        val cpuMetricFiles = resourceMonitorMetricDirectory.toFile()
            .walk()
            .filter { it.isFile && it.name.startsWith("proc-stat") }
        // Parse each file and associate it with a hostname
        val parser = ProcStatParser()
        for (cpuMetricFile in cpuMetricFiles) {
            val hostname = cpuMetricFile.name.split("-").last()
            cpuUtilizationData[hostname] = parser.parse(cpuMetricFile)
        }
    }

    private fun parseNetworkUtilization() {
        // Find relevant network metric files
        val networkMetricFiles = resourceMonitorMetricDirectory.toFile()
            .walk()
            .filter { it.isFile && it.name.startsWith("proc-net-dev") }
        // Parse each file and associate it with a hostname
        val parser = ProcNetDevParser()
        for (networkMetricFile in networkMetricFiles) {
            val hostname = networkMetricFile.name.split("-").last()
            networkUtilizationData[hostname] = parser.parse(networkMetricFile)
        }
    }

    private fun parseDiskUtilization() {
        // Find relevant disk metric files
        val diskMetricFiles = resourceMonitorMetricDirectory.toFile()
            .walk()
            .filter { it.isFile && it.name.startsWith("proc-diskstats") }
        // Parse each file and associate it with a hostname
        val parser = ProcDiskstatsParser()
        for (diskMetricFile in diskMetricFiles) {
            val hostname = diskMetricFile.name.split("-").last()
            diskUtilizationData[hostname] = parser.parse(diskMetricFile)
        }
    }

    companion object {

        fun parseFromDirectory(resourceMonitorMetricDirectory: Path): ResourceMonitorMetrics {
            return ResourceMonitorParser(resourceMonitorMetricDirectory).parse()
        }

    }

}

data class ResourceMonitorMetrics(
    val hostnames: Set<String>,
    val cpuUtilizationData: Map<String, CpuUtilizationData>,
    val networkUtilizationData: Map<String, NetworkUtilizationData>,
    val diskUtilizationData: Map<String, DiskUtilizationData>
)