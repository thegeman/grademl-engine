package science.atlarge.grademl.query.execution.operators

import org.junit.jupiter.api.BeforeAll
import science.atlarge.grademl.query.execution.DataGenerator
import science.atlarge.grademl.query.execution.GeneratedRow
import science.atlarge.grademl.query.execution.IndexedSortColumn
import science.atlarge.grademl.query.model.v2.*
import kotlin.test.Test
import kotlin.test.assertEquals

class SortOperatorTests {

    companion object {
        private var inputData: List<GeneratedRow> = emptyList()

        @JvmStatic
        @BeforeAll
        fun generateInputData() {
            inputData = DataGenerator.generate(10, 5)
        }
    }

    @Test
    fun testSortFirstKeyColumn() {
        testSort(listOf("k1"))
    }

    @Test
    fun testSortSecondKeyColumn() {
        testSort(listOf("-k2"))
    }

    @Test
    fun testSortBothKeyColumns() {
        testSort(listOf("-k1", "k2"))
    }

    @Test
    fun testSortNumericValueColumn() {
        testSort(listOf("v1"))
    }

    @Test
    fun testSortStringValueColumn() {
        testSort(listOf("-v2"))
    }

    @Test
    fun testSortBooleanValueColumn() {
        testSort(listOf("v3"))
    }

    @Test
    fun testSortMixedColumns() {
        testSort(listOf("k2", "v1", "k1"))
    }

    @Test
    fun testSortKeyColumnWithPreSort() {
        testSort(listOf("k2"), listOf("k1"))
    }

    @Test
    fun testSortNumericValueColumnWithPreSort() {
        testSort(listOf("-v1"), listOf("k2"))
    }

    @Test
    fun testSortStringValueColumnWithPreSort() {
        testSort(listOf("v2"), listOf("-k1"))
    }

    @Test
    fun testSortBooleanValueColumnWithPreSort() {
        testSort(listOf("-v3"), listOf("-k2"))
    }

    @Test
    fun testSortMixedColumnsWithPreSort() {
        testSort(listOf("-v3", "k2", "v1"), listOf("k1"))
    }

    private fun testSort(sortColumns: List<String>, preSortColumns: List<String> = emptyList()) {
        val shuffledInput = preSortInput(preSortColumns)
        val sortOperator = createSortOperator(
            shuffledInput, preSortColumns.map { sortColumn(it).columnIndex }, sortColumns.map { sortColumn(it) }
        )
        val expectedOutput = applySort(shuffledInput, preSortColumns + sortColumns)
        val producedOutput = sortOperator.execute()
        compareOutput(expectedOutput, producedOutput.toGeneratedRows())
    }

    private fun compareOutput(expected: List<List<GeneratedRow>>, produced: List<List<GeneratedRow>>) {
        // Check number of time series
        assertEquals(expected.size, produced.size, "Produced incorrect number of time series")
        // Check number of rows per time series
        expected.forEachIndexed { index, expectedRows ->
            val producedRows = produced[index]
            assertEquals(expectedRows.size, producedRows.size, "Produced incorrect number of rows")
        }
        // Check rows in time series
        expected.forEachIndexed { index, expectedRows ->
            val producedRows = produced[index]
            expectedRows.forEachIndexed { rowIndex, expectedRow ->
                val producedRow = producedRows[rowIndex]
                assertEquals(expectedRow, producedRow, "Produced incorrect row")
            }
        }
    }

    private fun TimeSeriesIterator.toGeneratedRows(): List<List<GeneratedRow>> {
        val result = mutableListOf<List<GeneratedRow>>()
        while (loadNext()) {
            val rows = mutableListOf<GeneratedRow>()
            val rowIter = currentTimeSeries.rowIterator()
            while (rowIter.loadNext()) {
                rows.add(GeneratedRow.fromQueryEngineRow(rowIter.currentRow))
            }
            result.add(rows)
        }
        return result
    }

    private fun applySort(
        orderedInput: List<List<GeneratedRow>>,
        columnsToSort: List<String>
    ): List<List<GeneratedRow>> {
        var reorderedInput = orderedInput
        for (c in columnsToSort.asReversed()) {
            when (c) {
                "k1" -> reorderedInput = reorderedInput.sortedBy { it[0].k1 }
                "-k1" -> reorderedInput = reorderedInput.sortedByDescending { it[0].k1 }
                "k2" -> reorderedInput = reorderedInput.sortedBy { it[0].k2 }
                "-k2" -> reorderedInput = reorderedInput.sortedByDescending { it[0].k2 }
                "v1" -> reorderedInput = reorderedInput.flatMap { it.groupBy { it.v1 }.map { it.value } }
                    .sortedBy { it[0].v1 }
                "-v1" -> reorderedInput = reorderedInput.flatMap { it.groupBy { it.v1 }.map { it.value } }
                    .sortedByDescending { it[0].v1 }
                "v2" -> reorderedInput = reorderedInput.flatMap { it.groupBy { it.v2 }.map { it.value } }
                    .sortedBy { it[0].v2 }
                "-v2" -> reorderedInput = reorderedInput.flatMap { it.groupBy { it.v2 }.map { it.value } }
                    .sortedByDescending { it[0].v2 }
                "v3" -> reorderedInput = reorderedInput.flatMap { it.groupBy { it.v3 }.map { it.value } }
                    .sortedBy { it[0].v3 }
                "-v3" -> reorderedInput = reorderedInput.flatMap { it.groupBy { it.v3 }.map { it.value } }
                    .sortedByDescending { it[0].v3 }
                "v4" -> reorderedInput = reorderedInput.flatMap { it.groupBy { it.v4 }.map { it.value } }
                    .sortedBy { it[0].v4 }
                "-v4" -> reorderedInput = reorderedInput.flatMap { it.groupBy { it.v4 }.map { it.value } }
                    .sortedByDescending { it[0].v4 }
            }
        }
        return reorderedInput
    }

    private fun sortColumn(columnName: String): IndexedSortColumn {
        val name = columnName.trimStart('-')
        val ascending = !columnName.startsWith('-')
        return IndexedSortColumn(
            DataGenerator.schema.columns.indexOfFirst { it.identifier == name }, ascending
        )
    }

    private fun preSortInput(columnsToSort: List<String>): List<List<GeneratedRow>> {
        // Group rows into time series and shuffle before sorting
        val sortedTimeSeries = inputData.groupBy(GeneratedRow::originalTimeSeriesId)
            .map { it.value }
            .shuffled()
            .toMutableList()
        for (c in columnsToSort.asReversed()) {
            when (c) {
                "k1" -> sortedTimeSeries.sortBy { it[0].k1 }
                "-k1" -> sortedTimeSeries.sortByDescending { it[0].k1 }
                "k2" -> sortedTimeSeries.sortBy { it[0].k2 }
                "-k2" -> sortedTimeSeries.sortByDescending { it[0].k2 }
            }
        }
        return sortedTimeSeries
    }

    private fun createSortOperator(
        inputData: List<List<GeneratedRow>>,
        preSortedColumns: List<Int>,
        columnsToSort: List<IndexedSortColumn>
    ): QueryOperator {
        val table = object : Table {
            override val schema: TableSchema
                get() = DataGenerator.schema

            override fun timeSeriesIterator() = createTimeSeriesIterator(inputData)
        }
        val tableScan = LinearTableScanOperator(table)
        val newSchema = TableSchema(
            DataGenerator.schema.columns.mapIndexed { i, column ->
                if (!column.isKey && columnsToSort.any { it.columnIndex == 1 }) column.copy(isKey = true)
                else column
            }
        )
        return SortOperator(tableScan, newSchema, preSortedColumns, columnsToSort)
    }

    private fun createTimeSeriesIterator(inputData: List<List<GeneratedRow>>) = object : TimeSeriesIterator {
        private val mappedData = inputData.map { ts -> ts.map { it.asQueryEngineRow() } }
        private var currentTimeSeriesIndex = -1

        override val schema: TableSchema = DataGenerator.schema
        override val currentTimeSeries: TimeSeries = object : TimeSeries {
            override val schema: TableSchema = DataGenerator.schema

            override fun getBoolean(columnIndex: Int): Boolean {
                require(schema.columns[columnIndex].isKey)
                return mappedData[currentTimeSeriesIndex][0].getBoolean(columnIndex)
            }

            override fun getNumeric(columnIndex: Int): Double {
                require(schema.columns[columnIndex].isKey)
                return mappedData[currentTimeSeriesIndex][0].getNumeric(columnIndex)
            }

            override fun getString(columnIndex: Int): String {
                require(schema.columns[columnIndex].isKey)
                return mappedData[currentTimeSeriesIndex][0].getString(columnIndex)
            }

            override fun rowIterator(): RowIterator = object : RowIterator {
                private val timeSeriesIndex = currentTimeSeriesIndex
                private var currentRowIndex = -1

                override val schema: TableSchema = DataGenerator.schema
                override val currentRow: Row
                    get() = mappedData[timeSeriesIndex][currentRowIndex]

                override fun loadNext(): Boolean {
                    if (currentRowIndex + 1 >= mappedData[timeSeriesIndex].size) return false
                    currentRowIndex++
                    return true
                }
            }
        }

        override fun loadNext(): Boolean {
            if (currentTimeSeriesIndex + 1 >= mappedData.size) return false
            currentTimeSeriesIndex++
            return true
        }
    }

}