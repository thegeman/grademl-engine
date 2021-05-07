package science.atlarge.grademl.core

import science.atlarge.grademl.core.attribution.ResourceAttribution
import science.atlarge.grademl.core.attribution.ResourceAttributionRuleProvider
import science.atlarge.grademl.core.execution.ExecutionModel
import science.atlarge.grademl.core.input.InputSource
import science.atlarge.grademl.core.resources.ResourceModel
import java.nio.file.Path

class GradeMLJob(
    inputDirectories: Iterable<Path>,
    outputDirectory: Path,
    inputSources: Iterable<InputSource>,
    attributionRuleProvider: (ExecutionModel, ResourceModel) -> ResourceAttributionRuleProvider
) {

    private val unifiedModels by lazy {
        val executionModel = ExecutionModel()
        val resourceModel = ResourceModel()
        for (inputSource in inputSources) {
            inputSource.parseJobData(inputDirectories, executionModel, resourceModel)
        }
        (executionModel to resourceModel)
    }

    val unifiedExecutionModel: ExecutionModel
        get() = unifiedModels.first
    val unifiedResourceModel: ResourceModel
        get() = unifiedModels.second

    val resourceAttribution by lazy {
        ResourceAttribution(
            unifiedExecutionModel,
            unifiedResourceModel,
            attributionRuleProvider(unifiedExecutionModel, unifiedResourceModel)
        )
    }

}