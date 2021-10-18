package science.atlarge.grademl.query.execution.operators

import science.atlarge.grademl.query.execution.IndexedSortColumn
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.v2.*

class SortOperator(
    private val input: QueryOperator,
    override val schema: TableSchema,
    private val preSortedColumns: List<Int>,
    private val remainingSortColumns: List<IndexedSortColumn>
) : QueryOperator {

    override fun execute(): TimeSeriesIterator = SortTimeSeriesIterator(
        input.execute(), schema, preSortedColumns.toIntArray(),
        remainingSortColumns.map { it.columnIndex }.toIntArray(),
        remainingSortColumns.map { it.ascending }.toBooleanArray()
    )

}

private class SortTimeSeriesIterator(
    private val inputIterator: TimeSeriesIterator,
    override val schema: TableSchema,
    private val preSortedColumns: IntArray,
    private val remainingSortColumns: IntArray,
    private val remainingSortColumnsAscending: BooleanArray
) : TimeSeriesIterator {

    // Cache types of columns
    private val columnTypes = IntArray(schema.columns.size) { i ->
        when (schema.columns[i].type) {
            Type.UNDEFINED -> throw IllegalArgumentException("Cannot sort UNDEFINED column")
            Type.BOOLEAN -> 0
            Type.NUMERIC -> 1
            Type.STRING -> 2
        }
    }

    // Materialized keys and values of rows
    // Keys are duplicated instead of stored separately per time series, because sorting may break up time series
    // TODO: Store keys per time series instead of per row
    private val cachedBooleanValues = Array(schema.columns.size) { i ->
        if (columnTypes[i] == 0) BooleanArray(INITIAL_CACHE_SIZE)
        else booleanArrayOf()
    }
    private val cachedNumericValues = Array(schema.columns.size) { i ->
        if (columnTypes[i] == 1) DoubleArray(INITIAL_CACHE_SIZE)
        else doubleArrayOf()
    }
    private val cachedStringValues = Array(schema.columns.size) { i ->
        if (columnTypes[i] == 2) arrayOfNulls<String?>(INITIAL_CACHE_SIZE)
        else emptyArray()
    }

    // Track which rows map to which time series
    private var currentTimeSeriesId = 0
    private var inputTimeSeriesIdPerRow = IntArray(INITIAL_CACHE_SIZE)

    // Track how many rows are cached or fit in the cache
    private var cachedRowCount = 0
    private var maxCachedRowCount = INITIAL_CACHE_SIZE

    // Identify different types of columns for faster access
    private val notPreSortedColumns = inputIterator.schema.columns.indices
        .filter { it !in preSortedColumns }
        .toIntArray()
    private val isColumnPreSorted = BooleanArray(schema.columns.size) { i -> i in preSortedColumns }
    private val reversedRemainingSortColumns = remainingSortColumns.reversedArray()

    // Track the state of the input iterator
    private var peekedNextInputRow = false

    // Track the state of the output iterator
    private var outputToInputRowMap = intArrayOf()
    private var outputTimeSeriesIdPerRow = intArrayOf()
    private var lastOutputTimeSeriesId = 0
    private var startRowOfLastOutputTimeSeries = Int.MAX_VALUE
    private val outputTimeSeries = SortTimeSeries(
        schema, cachedBooleanValues, cachedNumericValues, cachedStringValues, isColumnPreSorted, columnTypes,
        reversedRemainingSortColumns
    )
    override val currentTimeSeries: TimeSeries
        get() = outputTimeSeries

    override fun loadNext(): Boolean {
        // Check if there are rows in the cache to create a new time series from
        if (startRowOfLastOutputTimeSeries < cachedRowCount) {
            // Find the end of the last time series
            val startOfNextTimeSeries =
                findEndOfOutputTimeSeries(startRowOfLastOutputTimeSeries, lastOutputTimeSeriesId) + 1
            if (startOfNextTimeSeries < cachedRowCount) {
                // If there is another time series cached, prepare and return it
                lastOutputTimeSeriesId++
                startRowOfLastOutputTimeSeries = startOfNextTimeSeries
                outputTimeSeries.startTimeSeries(
                    inputTimeSeriesIdPerRow = inputTimeSeriesIdPerRow,
                    outputToInputRowMap = outputToInputRowMap,
                    outputTimeSeriesIdPerRow = outputTimeSeriesIdPerRow,
                    firstOutRowIndex = startRowOfLastOutputTimeSeries,
                    maxRowCount = cachedRowCount,
                    timeSeriesId = lastOutputTimeSeriesId
                )
                return true
            }
        }
        // Reset the time series and row cache
        lastOutputTimeSeriesId = 0
        startRowOfLastOutputTimeSeries = Int.MAX_VALUE
        cachedRowCount = 0
        // Check if there are more time series to process
        if (!peekedNextInputRow && !inputIterator.loadNext()) {
            // If not, clean up the cache and return
            cachedBooleanValues.fill(booleanArrayOf())
            cachedNumericValues.fill(doubleArrayOf())
            cachedStringValues.fill(emptyArray())
            inputTimeSeriesIdPerRow = intArrayOf()
            maxCachedRowCount = 0
            return false
        }
        // Cache the first time series
        cacheTimeSeries(inputIterator.currentTimeSeries, addPreSortedKeys = true)
        peekedNextInputRow = false
        // Read all subsequent time series with identical values for the pre-sorted columns
        while (inputIterator.loadNext()) {
            // Check if the next time series matches our cache time series in the pre-sorted columns
            if (!matchesPreSortedColumns(inputIterator.currentTimeSeries)) {
                peekedNextInputRow = true
                break
            }
            // Add the next time series to the cache
            cacheTimeSeries(inputIterator.currentTimeSeries, addPreSortedKeys = false)
        }
        // Sort the rows in the cache
        val newRowOrder = (0 until cachedRowCount).toMutableList()
        newRowOrder.sortWith(rowComparator())
        outputToInputRowMap = newRowOrder.toIntArray()
        // Create a cache for row-to-time-series-id mapping
        if (outputTimeSeriesIdPerRow.size < outputToInputRowMap.size) {
            outputTimeSeriesIdPerRow = IntArray(outputToInputRowMap.size)
        } else {
            outputTimeSeriesIdPerRow.fill(0, 0, outputToInputRowMap.size)
        }
        // Set up the output iterator and return the first time series
        lastOutputTimeSeriesId = 1
        startRowOfLastOutputTimeSeries = 0
        outputTimeSeries.startTimeSeries(
            inputTimeSeriesIdPerRow = inputTimeSeriesIdPerRow,
            outputToInputRowMap = outputToInputRowMap,
            outputTimeSeriesIdPerRow = outputTimeSeriesIdPerRow,
            firstOutRowIndex = startRowOfLastOutputTimeSeries,
            maxRowCount = cachedRowCount,
            timeSeriesId = lastOutputTimeSeriesId
        )
        return true
    }

    private fun cacheTimeSeries(timeSeries: TimeSeries, addPreSortedKeys: Boolean) {
        // Add pre-sorted keys to the cache if needed
        if (addPreSortedKeys) {
            for (c in preSortedColumns) {
                when (columnTypes[c]) {
                    0 -> cachedBooleanValues[c][0] = timeSeries.getBoolean(c)
                    1 -> cachedNumericValues[c][0] = timeSeries.getNumeric(c)
                    2 -> cachedStringValues[c][0] = timeSeries.getString(c)
                }
            }
        }
        // Iterate over the time series's rows and cache them
        val rowIterator = timeSeries.rowIterator()
        val lCurrentTimeSeriesId = currentTimeSeriesId
        var lTimeSeriesIdPerRow = inputTimeSeriesIdPerRow
        var lCachedRowCount = cachedRowCount
        var lMaxCachedRowCount = maxCachedRowCount
        while (rowIterator.loadNext()) {
            // Expand the row cache if needed
            if (lCachedRowCount == lMaxCachedRowCount) {
                cachedRowCount = lCachedRowCount
                expandRowCache()
                lMaxCachedRowCount = maxCachedRowCount
                lTimeSeriesIdPerRow = inputTimeSeriesIdPerRow
            }
            // Add values of the row to the cache
            val row = rowIterator.currentRow
            for (c in notPreSortedColumns) {
                when (columnTypes[c]) {
                    0 -> cachedBooleanValues[c][lCachedRowCount] = row.getBoolean(c)
                    1 -> cachedNumericValues[c][lCachedRowCount] = row.getNumeric(c)
                    2 -> cachedStringValues[c][lCachedRowCount] = row.getString(c)
                }
            }
            lTimeSeriesIdPerRow[lCachedRowCount] = lCurrentTimeSeriesId
            lCachedRowCount++
        }
        cachedRowCount = lCachedRowCount
        currentTimeSeriesId++
    }

    private fun expandRowCache() {
        maxCachedRowCount *= 2
        for (c in notPreSortedColumns) {
            when (columnTypes[c]) {
                0 -> cachedBooleanValues[c] = cachedBooleanValues[c].copyOf(maxCachedRowCount)
                1 -> cachedNumericValues[c] = cachedNumericValues[c].copyOf(maxCachedRowCount)
                2 -> cachedStringValues[c] = cachedStringValues[c].copyOf(maxCachedRowCount)
            }
        }
        inputTimeSeriesIdPerRow = inputTimeSeriesIdPerRow.copyOf(maxCachedRowCount)
    }

    private fun matchesPreSortedColumns(timeSeries: TimeSeries): Boolean {
        // Compare pre-sorted column values against the first cached values
        for (c in preSortedColumns) {
            when (columnTypes[c]) {
                0 -> if (cachedBooleanValues[c][0] != timeSeries.getBoolean(c)) return false
                1 -> if (cachedNumericValues[c][0] != timeSeries.getNumeric(c)) return false
                2 -> if (cachedStringValues[c][0] != timeSeries.getString(c)) return false
            }
        }
        return true
    }

    private fun findEndOfOutputTimeSeries(firstOutputRow: Int, timeSeriesId: Int): Int {
        val iterator = SortRowIterator(
            schema = schema,
            cachedBooleanValues = cachedBooleanValues,
            cachedNumericValues = cachedNumericValues,
            cachedStringValues = cachedStringValues,
            isColumnPreSorted = isColumnPreSorted,
            reversedRemainingSortColumns = reversedRemainingSortColumns,
            columnTypes = columnTypes,
            maxRowCount = cachedRowCount,
            inRowToTsMap = inputTimeSeriesIdPerRow,
            outRowToTsMap = outputTimeSeriesIdPerRow,
            outTimeSeriesId = timeSeriesId,
            outToInRowMap = outputToInputRowMap,
            firstOutRowId = firstOutputRow
        )
        var lastMatchingRow = firstOutputRow - 1
        while (iterator.loadNext()) lastMatchingRow++
        return lastMatchingRow
    }

    private fun rowComparator() = object : Comparator<Int> {
        private val columns = remainingSortColumns
        private val localColumnTypes = columnTypes
        private val columnDirectionMultipliers = IntArray(remainingSortColumnsAscending.size) {
            if (remainingSortColumnsAscending[it]) 1 else -1
        }

        override fun compare(leftIndex: Int, rightIndex: Int): Int {
            columns.forEachIndexed { index, columnIndex ->
                val result = when (localColumnTypes[columnIndex]) {
                    0 -> cachedBooleanValues[columnIndex][leftIndex].compareTo(cachedBooleanValues[columnIndex][rightIndex])
                    1 -> cachedNumericValues[columnIndex][leftIndex].compareTo(cachedNumericValues[columnIndex][rightIndex])
                    2 -> cachedStringValues[columnIndex][leftIndex]!!.compareTo(cachedStringValues[columnIndex][rightIndex]!!)
                    else -> throw IllegalStateException()
                }
                if (result != 0) return result * columnDirectionMultipliers[index]
            }
            return leftIndex.compareTo(rightIndex)
        }
    }

    companion object {
        private const val INITIAL_CACHE_SIZE = 16
    }

}

private class SortTimeSeries(
    override val schema: TableSchema,
    private val cachedBooleanValues: Array<BooleanArray>,
    private val cachedNumericValues: Array<DoubleArray>,
    private val cachedStringValues: Array<Array<String?>>,
    private val isColumnPreSorted: BooleanArray,
    private val columnTypes: IntArray,
    private val reversedRemainingSortColumns: IntArray
) : TimeSeries {

    private var inRowToTsMap = intArrayOf()
    private var outToInRowMap = intArrayOf()
    private var outRowToTsMap = intArrayOf()
    private var firstOutRowId = -1
    private var firstInRowId = -1
    private var maxRowCount = -1
    private var outTimeSeriesId = -1

    fun startTimeSeries(
        inputTimeSeriesIdPerRow: IntArray,
        outputToInputRowMap: IntArray,
        outputTimeSeriesIdPerRow: IntArray,
        firstOutRowIndex: Int,
        maxRowCount: Int,
        timeSeriesId: Int
    ) {
        inRowToTsMap = inputTimeSeriesIdPerRow
        outToInRowMap = outputToInputRowMap
        outRowToTsMap = outputTimeSeriesIdPerRow
        firstOutRowId = firstOutRowIndex
        firstInRowId = outputToInputRowMap[firstOutRowIndex]
        this.maxRowCount = maxRowCount
        this.outTimeSeriesId = timeSeriesId
    }

    override fun getBoolean(columnIndex: Int): Boolean {
        return if (isColumnPreSorted[columnIndex]) cachedBooleanValues[columnIndex][0]
        else cachedBooleanValues[columnIndex][firstInRowId]
    }

    override fun getNumeric(columnIndex: Int): Double {
        return if (isColumnPreSorted[columnIndex]) cachedNumericValues[columnIndex][0]
        else cachedNumericValues[columnIndex][firstInRowId]
    }

    override fun getString(columnIndex: Int): String {
        return if (isColumnPreSorted[columnIndex]) cachedStringValues[columnIndex][0]!!
        else cachedStringValues[columnIndex][firstInRowId]!!
    }

    override fun rowIterator(): RowIterator {
        return SortRowIterator(
            schema = schema,
            cachedBooleanValues = cachedBooleanValues,
            cachedNumericValues = cachedNumericValues,
            cachedStringValues = cachedStringValues,
            isColumnPreSorted = isColumnPreSorted,
            reversedRemainingSortColumns = reversedRemainingSortColumns,
            columnTypes = columnTypes,
            maxRowCount = maxRowCount,
            inRowToTsMap = inRowToTsMap,
            outRowToTsMap = outRowToTsMap,
            outTimeSeriesId = outTimeSeriesId,
            outToInRowMap = outToInRowMap,
            firstOutRowId = firstOutRowId
        )
    }

}

private class SortRowIterator(
    override val schema: TableSchema,
    private val cachedBooleanValues: Array<BooleanArray>,
    private val cachedNumericValues: Array<DoubleArray>,
    private val cachedStringValues: Array<Array<String?>>,
    private val isColumnPreSorted: BooleanArray,
    private val reversedRemainingSortColumns: IntArray,
    private val columnTypes: IntArray,
    private val maxRowCount: Int,
    private val inRowToTsMap: IntArray,
    private val outRowToTsMap: IntArray,
    private val outTimeSeriesId: Int,
    private val outToInRowMap: IntArray,
    private val firstOutRowId: Int
) : RowIterator {

    private val firstInRowId = outToInRowMap[firstOutRowId]
    private val inTimeSeriesId = inRowToTsMap[firstInRowId]

    private var currentOutRowId = firstOutRowId - 1
    private var currentInRowId = -1

    override val currentRow = object : Row {
        override val schema: TableSchema
            get() = this@SortRowIterator.schema

        override fun getBoolean(columnIndex: Int): Boolean {
            return if (isColumnPreSorted[columnIndex]) cachedBooleanValues[columnIndex][0]
            else cachedBooleanValues[columnIndex][currentInRowId]
        }

        override fun getNumeric(columnIndex: Int): Double {
            return if (isColumnPreSorted[columnIndex]) cachedNumericValues[columnIndex][0]
            else cachedNumericValues[columnIndex][currentInRowId]
        }

        override fun getString(columnIndex: Int): String {
            return if (isColumnPreSorted[columnIndex]) cachedStringValues[columnIndex][0]!!
            else cachedStringValues[columnIndex][currentInRowId]!!
        }
    }

    override fun loadNext(): Boolean {
        // Check if there is a next row
        currentOutRowId++
        if (currentOutRowId >= maxRowCount) {
            currentOutRowId--
            return false
        }
        // Lookup the input row corresponding to the next output row
        currentInRowId = outToInRowMap[currentOutRowId]
        // Check if this is the first row of the time series
        if (currentOutRowId == firstOutRowId) {
            // Cache the time series ID of this row
            outRowToTsMap[currentOutRowId] = outTimeSeriesId
            return true
        }
        // Check if the next row is already marked as part of the same or a different time series
        val cachedTsId = outRowToTsMap[currentOutRowId]
        if (cachedTsId == outTimeSeriesId) return true
        else if (cachedTsId != 0) {
            currentOutRowId--
            return false
        }
        // Check if the next row is part of the same input time series
        val lInRowId = currentInRowId
        if (inRowToTsMap[lInRowId] != inTimeSeriesId) {
            outRowToTsMap[currentOutRowId] = outTimeSeriesId + 1
            currentOutRowId--
            return false
        }
        // Compare the sorted column values
        for (c in reversedRemainingSortColumns) {
            val columnIsEqual = when (columnTypes[c]) {
                0 -> cachedBooleanValues[c][lInRowId] == cachedBooleanValues[c][firstInRowId]
                1 -> cachedNumericValues[c][lInRowId] == cachedNumericValues[c][firstInRowId]
                2 -> cachedStringValues[c][lInRowId] == cachedStringValues[c][firstInRowId]
                else -> throw IllegalStateException()
            }
            // If any sorted column is different in value, it starts the next time series
            if (!columnIsEqual) {
                outRowToTsMap[currentOutRowId] = outTimeSeriesId + 1
                currentOutRowId--
                return false
            }
        }
        // If all sorted column values match, mark the new row as part of the ongoing time series
        outRowToTsMap[currentOutRowId] = outTimeSeriesId
        return true
    }

}