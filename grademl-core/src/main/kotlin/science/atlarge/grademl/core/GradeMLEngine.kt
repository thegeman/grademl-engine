package science.atlarge.grademl.core

import science.atlarge.grademl.core.attribution.BestFitAttributionRuleProvider
import science.atlarge.grademl.core.input.InputSource
import java.nio.file.Path

object GradeMLEngine {

    private val knownInputSources = mutableSetOf<InputSource>()

    fun analyzeJob(
        jobDataDirectories: Iterable<Path>,
        jobOutputDirectory: Path,
        progressReport: (GradeMLJobStatusUpdate) -> Unit = { }
    ): GradeMLJob {
        return GradeMLJobProcessor.processJob(
            jobDataDirectories,
            jobOutputDirectory,
            knownInputSources,
            { em, rm -> BestFitAttributionRuleProvider.from(em, rm, jobOutputDirectory) },
            progressReport
        )
    }

    fun registerInputSource(inputSource: InputSource) {
        knownInputSources.add(inputSource)
    }

}