package science.atlarge.grademl.core

import science.atlarge.grademl.core.attribution.ResourceAttribution
import science.atlarge.grademl.core.execution.ExecutionModel
import science.atlarge.grademl.core.resources.ResourceModel

class GradeMLJob(
    val unifiedExecutionModel: ExecutionModel,
    val unifiedResourceModel: ResourceModel,
    val resourceAttribution: ResourceAttribution
)