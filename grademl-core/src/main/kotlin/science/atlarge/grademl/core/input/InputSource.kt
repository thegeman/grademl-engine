package science.atlarge.grademl.core.input

import science.atlarge.grademl.core.execution.ExecutionModel
import science.atlarge.grademl.core.resources.ResourceModel
import java.nio.file.Path

interface InputSource {

    fun parseJobData(
        jobDataDirectories: Iterable<Path>,
        unifiedExecutionModel: ExecutionModel,
        unifiedResourceModel: ResourceModel
    ): Boolean

}