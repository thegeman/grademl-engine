package science.atlarge.grademl.query.execution.data

import science.atlarge.grademl.core.GradeMLJob
import science.atlarge.grademl.core.TimestampNs
import science.atlarge.grademl.core.models.execution.ExecutionPhase
import science.atlarge.grademl.query.analysis.ASTAnalysis
import science.atlarge.grademl.query.execution.ExpressionEvaluation
import science.atlarge.grademl.query.execution.IndexedSortColumn
import science.atlarge.grademl.query.execution.SortColumn
import science.atlarge.grademl.query.execution.scanners.FilteringScanner
import science.atlarge.grademl.query.execution.scanners.RemappingScanner
import science.atlarge.grademl.query.language.*
import science.atlarge.grademl.query.model.*
import science.atlarge.grademl.query.nextOrNull

class PhasesTable private constructor(
    private val gradeMLJob: GradeMLJob,
    private val selectedColumnIds: List<Int>,
    private val filterCondition: Expression?,
    private val sortColumns: List<IndexedSortColumn>
) : Table {

    override val columns = selectedColumnIds.map { i -> COLUMNS[i] }

    override val columnsOptimizedForFilter = COLUMNS

    override val columnsOptimizedForSort = COLUMNS

    constructor(gradeMLJob: GradeMLJob) : this(gradeMLJob, COLUMNS.indices.toList(), null, emptyList())

    override fun scan(): RowScanner {
        // Create a list of phases
        val phases = gradeMLJob.unifiedExecutionModel.rootPhase.descendants.toMutableList()
        // Sort the phases if needed
        for (sortColumn in sortColumns.asReversed()) {
            when (sortColumn.columnIndex) {
                COLUMN_START_TIME -> {
                    if (sortColumn.ascending) phases.sortBy { it.startTime }
                    else phases.sortByDescending { it.startTime }
                }
                COLUMN_END_TIME -> {
                    if (sortColumn.ascending) phases.sortBy { it.endTime }
                    else phases.sortByDescending { it.endTime }
                }
                COLUMN_PATH -> {
                    if (sortColumn.ascending) phases.sortBy { it.path }
                    else phases.sortByDescending { it.path }
                }
                COLUMN_TYPE -> {
                    if (sortColumn.ascending) phases.sortBy { it.type.path }
                    else phases.sortByDescending { it.type.path }
                }
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
        return PhasesTable(gradeMLJob, newSelectedColumnIds, filterCondition, sortColumns)
    }

    override fun filteredWith(condition: Expression): Table {
        val newFilterCondition = ASTAnalysis.analyzeExpression(
            if (filterCondition == null) condition else BinaryExpression(condition, filterCondition, BinaryOp.AND),
            COLUMNS
        )
        return PhasesTable(gradeMLJob, selectedColumnIds, newFilterCondition, sortColumns)
    }

    override fun sortedBy(sortColumns: List<SortColumn>): Table {
        val addedSortColumns = sortColumns.map { c ->
            val index = columns.indexOfFirst { it.name == c.column.columnName }
            require(index in selectedColumnIds.indices) { "Cannot sort by column that is not selected" }
            IndexedSortColumn(selectedColumnIds[index], c.ascending)
        }

        val combinedSortColumns = mutableListOf<IndexedSortColumn>()
        val usedColumnIds = mutableSetOf<Int>()
        for (c in addedSortColumns + this.sortColumns) {
            if (c.columnIndex !in usedColumnIds) {
                combinedSortColumns.add(c)
                usedColumnIds.add(c.columnIndex)
            }
        }

        return PhasesTable(gradeMLJob, selectedColumnIds, filterCondition, combinedSortColumns)
    }

    private fun createPhaseRows(phases: List<ExecutionPhase>): List<Row> {
        val startTime = gradeMLJob.unifiedExecutionModel.rootPhase.startTime
        return phases.map { PhasesTableRow(it, startTime) }
    }

    companion object {
        const val COLUMN_START_TIME = Column.INDEX_START_TIME
        const val COLUMN_END_TIME =   Column.INDEX_END_TIME
        const val COLUMN_DURATION =   Column.INDEX_DURATION
        const val COLUMN_PATH =       Column.RESERVED_COLUMNS
        const val COLUMN_TYPE =       Column.RESERVED_COLUMNS + 1

        val COLUMNS = listOf(
            Column.START_TIME,
            Column.END_TIME,
            Column.DURATION,
            Column("path", "path", COLUMN_PATH, Type.STRING, true),
            Column("type", "type", COLUMN_TYPE, Type.STRING, true)
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
            PhasesTable.COLUMN_START_TIME -> outValue.numericValue = (phase.startTime - deltaTs) * (1 / 1e9)
            PhasesTable.COLUMN_END_TIME -> outValue.numericValue = (phase.endTime - deltaTs) * (1 / 1e9)
            PhasesTable.COLUMN_DURATION -> outValue.numericValue = (phase.endTime - phase.startTime) * (1 / 1e9)
            PhasesTable.COLUMN_PATH -> outValue.stringValue = phase.path.toString()
            PhasesTable.COLUMN_TYPE -> outValue.stringValue = phase.type.path.toString()
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