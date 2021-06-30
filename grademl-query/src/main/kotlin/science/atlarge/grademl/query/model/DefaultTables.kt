package science.atlarge.grademl.query.model

import science.atlarge.grademl.core.GradeMLJob
import science.atlarge.grademl.query.model.impl.AttributedMetricsTable
import science.atlarge.grademl.query.model.impl.MetricsTable
import science.atlarge.grademl.query.model.impl.PhasesTable

object DefaultTables {

    fun create(gradeMLJob: GradeMLJob): Map<String, Table> {
        return mapOf(
            "metrics" to MetricsTable(gradeMLJob),
            "phases" to PhasesTable(),
            "attributed_metrics" to AttributedMetricsTable()
        )
    }

}