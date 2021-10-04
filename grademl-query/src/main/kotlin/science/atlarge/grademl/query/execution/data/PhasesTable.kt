package science.atlarge.grademl.query.execution.data

import science.atlarge.grademl.core.GradeMLJob
import science.atlarge.grademl.core.TimestampNs
import science.atlarge.grademl.core.models.execution.ExecutionPhase
import science.atlarge.grademl.query.analysis.ASTAnalysis
import science.atlarge.grademl.query.execution.ExpressionEvaluation
import science.atlarge.grademl.query.execution.scanners.FilteringScanner
import science.atlarge.grademl.query.execution.scanners.RemappingScanner
import science.atlarge.grademl.query.language.*
import science.atlarge.grademl.query.model.*
import science.atlarge.grademl.query.nextOrNull

class PhasesTable private constructor(
    private val gradeMLJob: GradeMLJob,
    private val selectedColumnIds: List<Int>,
    private val filterCondition: Expression?,
    private val sortColumnIds: List<Int>
) : Table {

    override val columns = selectedColumnIds.map { i -> COLUMNS[i] }

    override val columnsOptimizedForFilter = COLUMNS

    override val columnsOptimizedForSort = COLUMNS

    constructor(gradeMLJob: GradeMLJob) : this(gradeMLJob, COLUMNS.indices.toList(), null, emptyList())

    override fun scan(): RowScanner {
        // Create a list of phases
        val phases = gradeMLJob.unifiedExecutionModel.rootPhase.descendants.toMutableList()
        // Sort the phases if needed
        for (columnId in sortColumnIds.asReversed()) {
            when (columnId) {
                0 -> /* start_time */ phases.sortBy { it.startTime }
                1 -> /* end_time */ phases.sortBy { it.endTime }
                2 -> /* path */ phases.sortBy { it.path }
                3 -> /* type */ phases.sortBy { it.type.path }
            }
        }
        // Wrap each phase in a Row object
        val phaseRows = createPhaseRows(phases)
        // Create a linear scanner of all phases
        var scanner: RowScanner = object : RowScanner() {
            val iterator = phaseRows.iterator()
            override fun fetchRow() = iterator.nextOrNull()
        }
        // Filter if needed
        if (filterCondition != null) {
            val filterResult = TypedValue()
            scanner = FilteringScanner(scanner) { row ->
                ExpressionEvaluation.evaluate(filterCondition, row, filterResult).booleanValue
            }
        }
        // Remap columns if needed
        if (selectedColumnIds != COLUMNS.indices.toList()) {
            scanner = RemappingScanner(scanner, selectedColumnIds)
        }

        return scanner
    }

    override fun withSubsetColumns(subsetColumns: List<ColumnLiteral>): Table {
        val newSelectedColumnIds = subsetColumns.map { c ->
            val index = columns.indexOfFirst { it.name == c.columnName }
            require(index in selectedColumnIds.indices)
            selectedColumnIds[index]
        }
        return PhasesTable(gradeMLJob, newSelectedColumnIds, filterCondition, sortColumnIds)
    }

    override fun filteredWith(condition: Expression): Table {
        val newFilterCondition = ASTAnalysis.analyzeExpression(
            if (filterCondition == null) condition else BinaryExpression(condition, filterCondition, BinaryOp.AND),
            COLUMNS
        )
        return PhasesTable(gradeMLJob, selectedColumnIds, newFilterCondition, sortColumnIds)
    }

    override fun sortedBy(sortColumns: List<ColumnLiteral>): Table {
        val columnIds = sortColumns.map { c ->
            val index = columns.indexOfFirst { it.name == c.columnName }
            require(index in selectedColumnIds.indices)
            selectedColumnIds[index]
        }
        val newSortColumnIds = (columnIds + sortColumnIds).distinct()
        return PhasesTable(gradeMLJob, selectedColumnIds, filterCondition, newSortColumnIds)
    }

    private fun createPhaseRows(phases: List<ExecutionPhase>): List<Row> {
        val startTime = gradeMLJob.unifiedExecutionModel.rootPhase.startTime
        return phases.map { PhasesTableRow(it, startTime) }
    }

    companion object {
        val COLUMNS = listOf(
            Column("_start_time", "_start_time", Type.NUMERIC, ColumnFunction.TIME_START),
            Column("_end_time", "_end_time", Type.NUMERIC, ColumnFunction.TIME_END),
            Column("path", "path", Type.STRING, ColumnFunction.KEY),
            Column("type", "type", Type.STRING, ColumnFunction.METADATA)
        )
    }

}

private class PhasesTableRow(
    val phase: ExecutionPhase,
    val deltaTs: TimestampNs
) : Row {

    override val columnCount = PhasesTable.COLUMNS.size

    override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
        when (columnId) {
            0 -> /* start_time */ outValue.numericValue = (phase.startTime - deltaTs) * (1 / 1e9)
            1 -> /* end_time */ outValue.numericValue = (phase.endTime - deltaTs) * (1 / 1e9)
            2 -> /* path */ outValue.stringValue = phase.path.toString()
            3 -> /* type */ outValue.stringValue = phase.type.path.toString()
            else -> {
                require(columnId !in 0 until columnCount) {
                    "Mismatch between PhasesTableRow and PhasesTable.COLUMNS"
                }
                throw IndexOutOfBoundsException("Column $columnId does not exist: table has $columnCount columns")
            }
        }
        return outValue
    }

}