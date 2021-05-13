package science.atlarge.grademl.cli.util

import science.atlarge.grademl.cli.data.MetricListWriter
import science.atlarge.grademl.cli.data.PhaseListWriter
import science.atlarge.grademl.core.models.execution.ExecutionModel
import science.atlarge.grademl.core.models.execution.ExecutionPhase
import science.atlarge.grademl.core.models.resource.Metric
import science.atlarge.grademl.core.models.resource.ResourceModel
import java.nio.file.Path

class OutputPaths(
    private val outputDirectory: Path,
    private val phaseList: PhaseList,
    private val metricList: MetricList
) {

    init {
        outputDirectory.toFile().mkdirs()
    }

    fun pathForPhase(phase: ExecutionPhase): Path {
        return outputDirectory.resolve(phaseList.phaseToIdentifier(phase))
    }

    fun pathForMetric(metric: Metric): Path {
        return outputDirectory.resolve(metricList.metricToIdentifier(metric))
    }

    fun writeIndex(executionModel: ExecutionModel, resourceModel: ResourceModel, jobTime: JobTime) {
        PhaseListWriter.output(
            outputDirectory.resolve(PhaseListWriter.FILENAME).toFile(),
            executionModel.rootPhase,
            executionModel.phases,
            phaseList,
            jobTime
        )
        MetricListWriter.output(
            outputDirectory.resolve(MetricListWriter.FILENAME).toFile(),
            resourceModel.resources.flatMap { it.metrics },
            metricList
        )
    }

}