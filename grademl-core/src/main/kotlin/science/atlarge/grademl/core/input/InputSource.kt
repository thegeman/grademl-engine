package science.atlarge.grademl.core.input

import science.atlarge.grademl.core.models.Environment
import science.atlarge.grademl.core.models.execution.ExecutionModel
import science.atlarge.grademl.core.models.resource.ResourceModel
import java.nio.file.Path

interface InputSource {

    fun parseJobData(
        jobDataDirectories: Iterable<Path>,
        unifiedExecutionModel: ExecutionModel,
        unifiedResourceModel: ResourceModel,
        jobEnvironment: Environment
    ): Boolean

}