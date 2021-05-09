package science.atlarge.grademl.input.spark

import science.atlarge.grademl.core.execution.ExecutionModel
import science.atlarge.grademl.core.input.InputSource
import science.atlarge.grademl.core.resources.ResourceModel
import java.nio.file.Path

object Spark : InputSource {

    override fun parseJobData(
        jobDataDirectories: Iterable<Path>,
        unifiedExecutionModel: ExecutionModel,
        unifiedResourceModel: ResourceModel
    ): Boolean {
        // Find Spark log directories
        val sparkLogDirectories = jobDataDirectories
            .map { it.resolve("logs").resolve("spark") }
            .filter { it.toFile().isDirectory }
        if (sparkLogDirectories.isEmpty()) return false

        // Parse Spark log files
        val sparkLog = SparkLogParser.parseFromDirectories(sparkLogDirectories)

        // TODO: Convert Spark logs to phases in the execution model
        return true
    }

}