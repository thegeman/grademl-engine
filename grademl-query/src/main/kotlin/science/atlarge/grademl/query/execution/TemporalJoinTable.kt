package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.model.Column
import science.atlarge.grademl.query.model.ColumnFunction
import science.atlarge.grademl.query.model.RowScanner
import science.atlarge.grademl.query.model.Table

class TemporalJoinTable private constructor(
    private val inputTables: List<Table>,
    private val inputTimeColumns: List<Map<ColumnFunction, Column>>
) : Table {

    override val isGrouped = false

    override val columns: List<Column>
        get() = TODO("Not yet implemented")

    override fun scan(): RowScanner {
        TODO("Not yet implemented")
    }

    companion object {

        fun from(inputTables: List<Table>): Table {
            require(inputTables.size >= 2) { "Cannot join less than two tables together" }

            // Ensure that none of the input tables have overlapping column names,
            // except for the special start_time and end_time columns
            val allColumnsByPath = mutableMapOf<String, Column>()
            for (inputTable in inputTables) {
                for (column in inputTable.columns) {
                    if (column.path == "start_time" || column.path == "end_time") continue
                    require(column.path !in allColumnsByPath) {
                        "Cannot join tables with duplicate column: ${column.path}"
                    }
                    allColumnsByPath[column.path] = column
                }
            }

            // Identify the time-related columns in each table
            val inputTimeColumns = inputTables.map { inputTable ->
                val columns = inputTable.columns
                // Find a start_time column
                val startTimeColumns = columns.filter { it.function == ColumnFunction.TIME_START }
                require(startTimeColumns.size == 1) {
                    "Inputs of a TEMPORAL JOIN must have precisely one column indicating the start time of an event"
                }
                // Find an end_time column
                val endTimeColumns = columns.filter { it.function == ColumnFunction.TIME_END }
                require(endTimeColumns.size == 1) {
                    "Inputs of a TEMPORAL JOIN must have precisely one column indicating the end time of an event"
                }

                mapOf(ColumnFunction.TIME_START to startTimeColumns[0], ColumnFunction.TIME_END to endTimeColumns[0])
            }

            TODO()
        }

    }

}