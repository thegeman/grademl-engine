package science.atlarge.grademl.query.model

object DefaultTables {

    fun create(): Map<String, Table> {
        return mapOf(
            "metrics" to MetricsTable(),
            "phases" to PhasesTable(),
            "attributed_metrics" to AttributedMetricsTable()
        )
    }

}