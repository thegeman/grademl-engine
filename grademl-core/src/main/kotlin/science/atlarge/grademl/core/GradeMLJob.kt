package science.atlarge.grademl.core

import science.atlarge.grademl.core.attribution.ResourceAttribution
import science.atlarge.grademl.core.models.execution.ExecutionModel
import science.atlarge.grademl.core.models.resource.ResourceModel

class GradeMLJob(
    val unifiedExecutionModel: ExecutionModel,
    val unifiedResourceModel: ResourceModel,
    val resourceAttribution: ResourceAttribution
)