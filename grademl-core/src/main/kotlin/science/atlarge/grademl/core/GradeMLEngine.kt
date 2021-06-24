package science.atlarge.grademl.core

import science.atlarge.grademl.core.attribution.BestFitAttributionRuleProvider
import science.atlarge.grademl.core.attribution.CachingAttributionRuleProvider
import science.atlarge.grademl.core.attribution.MappingAttributionRuleProvider
import science.atlarge.grademl.core.attribution.ResourceAttributionRuleProvider
import science.atlarge.grademl.core.input.EnvironmentParser
import science.atlarge.grademl.core.input.InputSource
import science.atlarge.grademl.core.models.CommonMetadata
import science.atlarge.grademl.core.models.Environment
import science.atlarge.grademl.core.models.execution.ExecutionModel
import science.atlarge.grademl.core.models.resource.ResourceModel
import java.nio.file.Path

object GradeMLEngine {

    private val knownInputSources = mutableSetOf<InputSource>(
        EnvironmentParser
    )

    fun analyzeJob(
        jobDataDirectories: Iterable<Path>,
        jobOutputDirectory: Path,
        progressReport: (GradeMLJobStatusUpdate) -> Unit = { }
    ): GradeMLJob {
        fun createAttributionRuleProvider(
            executionModel: ExecutionModel,
            resourceModel: ResourceModel,
            environment: Environment
        ): ResourceAttributionRuleProvider {
            val machineMapping = MappingAttributionRuleProvider.Mapping(CommonMetadata.MACHINE_ID) { id1, id2 ->
                val machine1 = environment.machineForId(id1)
                val machine2 = environment.machineForId(id2)
                if (machine1 != null && machine2 != null) machine1 === machine2
                else id1 == id2
            }
            return CachingAttributionRuleProvider(
                jobOutputDirectory,
                BestFitAttributionRuleProvider.from(
                    executionModel,
                    resourceModel,
                    jobOutputDirectory,
                    MappingAttributionRuleProvider(listOf(machineMapping))
                )
            )
        }

        return GradeMLJobProcessor.processJob(
            jobDataDirectories,
            jobOutputDirectory,
            knownInputSources,
            ::createAttributionRuleProvider,
            progressReport
        )
    }

    fun registerInputSource(inputSource: InputSource) {
        knownInputSources.add(inputSource)
    }

}