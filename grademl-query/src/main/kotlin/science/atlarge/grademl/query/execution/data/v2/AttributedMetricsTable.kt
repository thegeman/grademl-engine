package science.atlarge.grademl.query.execution.data.v2

import science.atlarge.grademl.core.GradeMLJob
import science.atlarge.grademl.core.attribution.AttributedResourceData
import science.atlarge.grademl.core.models.execution.ExecutionPhase
import science.atlarge.grademl.core.models.resource.Metric
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.v2.*

class AttributedMetricsTable(
    private val gradeMLJob: GradeMLJob
) : Table {

    override val schema = TableSchema(COLUMNS)

    override fun timeSeriesIterator(): TimeSeriesIterator {
        val allMetricPhasePairs = gradeMLJob.unifiedResourceModel.rootResource.metricsInTree.asSequence()
            .flatMap { metric -> gradeMLJob.unifiedExecutionModel.phases.map { phase -> metric to phase } }
            .iterator()
        val firstTimestampNs = gradeMLJob.unifiedExecutionModel.rootPhase.startTime
        return object : TimeSeriesIterator {
            override val schema: TableSchema
                get() = this@AttributedMetricsTable.schema
            override val currentTimeSeries: TimeSeries
                get() = metricPhaseTimeSeries

            private val metricPhaseTimeSeries = object : TimeSeries {
                lateinit var metric: Metric
                lateinit var phase: ExecutionPhase
                lateinit var attributedResourceData: AttributedResourceData

                override val schema: TableSchema
                    get() = this@AttributedMetricsTable.schema

                override fun getBoolean(columnIndex: Int): Boolean {
                    throw IllegalArgumentException(
                        "Column $columnIndex does not exist, is not a key, or is not BOOLEAN"
                    )
                }

                override fun getNumeric(columnIndex: Int): Double {
                    throw IllegalArgumentException(
                        "Column $columnIndex does not exist, is not a key, or is not NUMERIC"
                    )
                }

                override fun getString(columnIndex: Int): String {
                    return when (columnIndex) {
                        INDEX_METRIC_PATH -> metric.path.toString()
                        INDEX_METRIC_TYPE -> metric.type.toString()
                        INDEX_PHASE_PATH -> phase.path.toString()
                        INDEX_PHASE_TYPE -> phase.type.toString()
                        else -> throw IllegalArgumentException(
                            "Column $columnIndex does not exist, is not a key, or is not STRING"
                        )
                    }
                }

                override fun rowIterator(): RowIterator {
                    val usageIterator = attributedResourceData.metricData.iterator()
                    val capacityIterator = attributedResourceData.availableCapacity.iterator()
                    return object : RowIterator {
                        override val schema: TableSchema
                            get() = this@AttributedMetricsTable.schema
                        override val currentRow = object : Row {
                            override val schema: TableSchema
                                get() = this@AttributedMetricsTable.schema

                            override fun getBoolean(columnIndex: Int): Boolean {
                                throw IllegalArgumentException(
                                    "Column $columnIndex does not exist, or is not BOOLEAN"
                                )
                            }

                            override fun getNumeric(columnIndex: Int): Double {
                                return when (columnIndex) {
                                    INDEX_START_TIME -> (usageIterator.currentStartTime - firstTimestampNs) / 1e9
                                    INDEX_END_TIME -> (usageIterator.currentEndTime - firstTimestampNs) / 1e9
                                    INDEX_DURATION -> (usageIterator.currentEndTime - usageIterator.currentStartTime) / 1e9
                                    INDEX_UTILIZATION ->
                                        if (capacityIterator.currentValue == 0.0) 0.0
                                        else usageIterator.currentValue / capacityIterator.currentValue
                                    INDEX_USAGE -> usageIterator.currentValue
                                    INDEX_CAPACITY -> capacityIterator.currentValue
                                    else -> throw IllegalArgumentException(
                                        "Column $columnIndex does not exist, or is not NUMERIC"
                                    )
                                }
                            }

                            override fun getString(columnIndex: Int): String {
                                return when (columnIndex) {
                                    INDEX_METRIC_PATH -> metric.path.toString()
                                    INDEX_METRIC_TYPE -> metric.type.toString()
                                    INDEX_PHASE_PATH -> phase.path.toString()
                                    INDEX_PHASE_TYPE -> phase.type.toString()
                                    else -> throw IllegalArgumentException(
                                        "Column $columnIndex does not exist, or is not STRING"
                                    )
                                }
                            }
                        }

                        override fun loadNext(): Boolean {
                            if (!usageIterator.hasNext) return false
                            usageIterator.next()
                            capacityIterator.next()
                            return true
                        }
                    }
                }
            }

            override fun loadNext(): Boolean {
                while (allMetricPhasePairs.hasNext()) {
                    val (metric, phase) = allMetricPhasePairs.next()
                    val attributionResult = gradeMLJob.resourceAttribution.attributeMetricToPhase(metric, phase)
                    if (attributionResult !is AttributedResourceData) continue
                    metricPhaseTimeSeries.metric = metric
                    metricPhaseTimeSeries.phase = phase
                    metricPhaseTimeSeries.attributedResourceData = attributionResult
                    return true
                }
                return false
            }
        }
    }

    companion object {
        const val INDEX_START_TIME = Columns.INDEX_START_TIME
        const val INDEX_END_TIME = Columns.INDEX_END_TIME
        const val INDEX_DURATION = Columns.INDEX_DURATION
        const val INDEX_UTILIZATION = Columns.INDEX_NOT_RESERVED
        const val INDEX_USAGE = Columns.INDEX_NOT_RESERVED + 1
        const val INDEX_CAPACITY = Columns.INDEX_NOT_RESERVED + 2
        const val INDEX_METRIC_PATH = Columns.INDEX_NOT_RESERVED + 3
        const val INDEX_METRIC_TYPE = Columns.INDEX_NOT_RESERVED + 4
        const val INDEX_PHASE_PATH = Columns.INDEX_NOT_RESERVED + 5
        const val INDEX_PHASE_TYPE = Columns.INDEX_NOT_RESERVED + 6

        val COLUMNS = listOf(
            Columns.START_TIME,
            Columns.END_TIME,
            Columns.DURATION,
            Column("utilization", Type.NUMERIC, false),
            Column("usage", Type.NUMERIC, false),
            Column("capacity", Type.NUMERIC, false),
            Column("metric_path", Type.STRING, true),
            Column("metric_type", Type.STRING, true),
            Column("phase_path", Type.STRING, true),
            Column("phase_type", Type.STRING, true)
        )
    }

}