package science.atlarge.grademl.core

import science.atlarge.grademl.core.analysis.BottleneckIdentification
import science.atlarge.grademl.core.attribution.ResourceAttribution
import science.atlarge.grademl.core.attribution.ResourceAttributionRuleProvider
import science.atlarge.grademl.core.input.InputSource
import science.atlarge.grademl.core.models.Environment
import science.atlarge.grademl.core.models.execution.ExecutionModel
import science.atlarge.grademl.core.models.resource.ResourceModel
import java.nio.file.Path

class GradeMLJobProcessor private constructor(
    private val inputDirectories: Iterable<Path>,
    private val outputDirectory: Path,
    private val inputSources: Iterable<InputSource>,
    private val attributionRuleProvider: (ExecutionModel, ResourceModel, Environment) -> ResourceAttributionRuleProvider,
    private val progressReport: (GradeMLJobStatusUpdate) -> Unit
) {

    private val executionModel = ExecutionModel()
    private val resourceModel = ResourceModel()
    private val jobEnvironment = Environment()
    private lateinit var resourceAttribution: ResourceAttribution

    fun run(): GradeMLJob {
        progressReport(GradeMLJobStatusUpdate.JOB_ANALYSIS_STARTING)

        // Parse logs
        progressReport(GradeMLJobStatusUpdate.LOG_PARSING_STARTING)
        for (inputSource in inputSources) {
            inputSource.parseJobData(inputDirectories, executionModel, resourceModel, jobEnvironment)
        }
        progressReport(GradeMLJobStatusUpdate.LOG_PARSING_COMPLETED)

        // Configure resource attribution
        resourceAttribution = ResourceAttribution(
            executionModel, resourceModel, attributionRuleProvider(executionModel, resourceModel, jobEnvironment)
        )

        // Configure bottleneck identification
        val bottleneckIdentification = BottleneckIdentification(
            resourceAttribution
        )

        progressReport(GradeMLJobStatusUpdate.JOB_ANALYSIS_COMPLETED)
        return GradeMLJob(executionModel, resourceModel, jobEnvironment, resourceAttribution, bottleneckIdentification)
    }

    companion object {
        fun processJob(
            inputDirectories: Iterable<Path>,
            outputDirectory: Path,
            inputSources: Iterable<InputSource>,
            attributionRuleProvider: (ExecutionModel, ResourceModel, Environment) -> ResourceAttributionRuleProvider,
            progressReport: (GradeMLJobStatusUpdate) -> Unit = { }
        ): GradeMLJob {
            return GradeMLJobProcessor(
                inputDirectories, outputDirectory, inputSources, attributionRuleProvider, progressReport
            ).run()
        }
    }

}