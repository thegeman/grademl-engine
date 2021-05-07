package science.atlarge.grademl.core

import science.atlarge.grademl.core.attribution.BestFitAttributionRuleProvider
import science.atlarge.grademl.core.input.InputSource
import java.nio.file.Path

object GradeMLEngine {

    private val knownInputSources = mutableSetOf<InputSource>()

    fun analyzeJob(jobDataDirectories: Iterable<Path>, jobOutputDirectory: Path): GradeMLJob {
        return GradeMLJob(
            jobDataDirectories,
            jobOutputDirectory,
            knownInputSources
        ) { executionModel, resourceModel ->
            BestFitAttributionRuleProvider.from(executionModel, resourceModel, jobOutputDirectory)
        }
    }

    fun registerInputSource(inputSource: InputSource) {
        knownInputSources.add(inputSource)
    }

}