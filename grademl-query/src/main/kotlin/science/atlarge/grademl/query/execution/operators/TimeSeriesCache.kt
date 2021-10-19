package science.atlarge.grademl.query.execution.operators

import science.atlarge.grademl.query.execution.operators.IntTypes.toInt
import science.atlarge.grademl.query.model.v2.*
import java.util.*

class TimeSeriesCache(
    private val schema: TableSchema
) {

    // Determine column types
    private val columnTypes = schema.columns.map { it.type.toInt() }.toIntArray()

    // Determine which column are keys and which are values
    private val isKeyColumn = schema.columns.map { it.isKey }.toBooleanArray()
    private val keyColumns = schema.columns
        .mapIndexed { index, column -> if (column.isKey) index else null }
        .filterNotNull()
        .toIntArray()
    private val valueColumns = schema.columns
        .mapIndexed { index, column -> if (!column.isKey) index else null }
        .filterNotNull()
        .toIntArray()

    // Cache time series keys and values
    private val cachedBooleanValues = columnTypes.map { columnType ->
        if (columnType == IntTypes.TYPE_BOOLEAN) BooleanArray(INITIAL_CACHE_SIZE) else booleanArrayOf()
    }.toTypedArray()
    private val cachedNumericValues = columnTypes.map { columnType ->
        if (columnType == IntTypes.TYPE_NUMERIC) DoubleArray(INITIAL_CACHE_SIZE) else doubleArrayOf()
    }.toTypedArray()
    private val cachedStringValues = columnTypes.map { columnType ->
        if (columnType == IntTypes.TYPE_STRING) arrayOfNulls<String?>(INITIAL_CACHE_SIZE) else emptyArray()
    }.toTypedArray()

    // Track how many rows are cached
    private var cachedRowCount = 0
    private var maxCachedRowCount = INITIAL_CACHE_SIZE

    // Track how many time series are cached and which rows map to which time series
    private var rowsPerTimeSeries = IntArray(INITIAL_CACHE_SIZE)
    private var cachedTimeSeriesCount = 0
    private var maxCachedTimeSeriesCount = INITIAL_CACHE_SIZE

    val numCachedTimeSeries: Int
        get() = cachedTimeSeriesCount
    val numCachedRows: Int
        get() = cachedRowCount

    fun addTimeSeries(timeSeries: TimeSeries) {
        // Prepare the time series-level cache
        if (cachedTimeSeriesCount == maxCachedTimeSeriesCount) expandTimeSeriesCache()
        val timeSeriesId = cachedTimeSeriesCount
        // Add keys to the cache
        for (c in keyColumns) {
            when (columnTypes[c]) {
                IntTypes.TYPE_BOOLEAN -> cachedBooleanValues[c][timeSeriesId] = timeSeries.getBoolean(c)
                IntTypes.TYPE_NUMERIC -> cachedNumericValues[c][timeSeriesId] = timeSeries.getNumeric(c)
                IntTypes.TYPE_STRING -> cachedStringValues[c][timeSeriesId] = timeSeries.getString(c)
            }
        }
        // Add rows to the cache
        val rowIterator = timeSeries.rowIterator()
        var addedRowCount = 0
        var lCachedRowCount = cachedRowCount
        var lMaxCachedRowCount = maxCachedRowCount
        while (rowIterator.loadNext()) {
            val row = rowIterator.currentRow
            // Expand the row cache if needed
            if (lCachedRowCount == lMaxCachedRowCount) {
                expandRowCache()
                lMaxCachedRowCount = maxCachedRowCount
            }
            // Add row values to the cache
            for (c in valueColumns) {
                when (columnTypes[c]) {
                    IntTypes.TYPE_BOOLEAN -> cachedBooleanValues[c][lCachedRowCount] = row.getBoolean(c)
                    IntTypes.TYPE_NUMERIC -> cachedNumericValues[c][lCachedRowCount] = row.getNumeric(c)
                    IntTypes.TYPE_STRING -> cachedStringValues[c][lCachedRowCount] = row.getString(c)
                }
            }
            // Increment row counters
            addedRowCount++
            lCachedRowCount++
        }

        // Store updated counters
        rowsPerTimeSeries[timeSeriesId] = addedRowCount
        cachedRowCount = lCachedRowCount
        cachedTimeSeriesCount++
    }

    fun iterator(): TimeSeriesIterator = object : TimeSeriesIterator {
        private var currentTimeSeriesId = -1
        private var currentTimeSeriesFirstRow = -1

        override val schema: TableSchema
            get() = this@TimeSeriesCache.schema
        override val currentTimeSeries: TimeSeries = object : TimeSeries {
            override val schema: TableSchema
                get() = this@TimeSeriesCache.schema

            override fun getBoolean(columnIndex: Int): Boolean {
                require(isKeyColumn[columnIndex])
                return cachedBooleanValues[columnIndex][currentTimeSeriesId]
            }

            override fun getNumeric(columnIndex: Int): Double {
                require(isKeyColumn[columnIndex])
                return cachedNumericValues[columnIndex][currentTimeSeriesId]
            }

            override fun getString(columnIndex: Int): String {
                require(isKeyColumn[columnIndex])
                return cachedStringValues[columnIndex][currentTimeSeriesId]!!
            }

            override fun rowIterator(): RowIterator = object : RowIterator {
                private val timeSeriesId = currentTimeSeriesId
                private var currentRowId = currentTimeSeriesFirstRow - 1
                private val lastRowId = currentTimeSeriesFirstRow + rowsPerTimeSeries[timeSeriesId] - 1

                override val schema: TableSchema
                    get() = this@TimeSeriesCache.schema
                override val currentRow: Row = object : Row {
                    override val schema: TableSchema
                        get() = this@TimeSeriesCache.schema

                    override fun getBoolean(columnIndex: Int): Boolean {
                        return if (isKeyColumn[columnIndex]) cachedBooleanValues[columnIndex][timeSeriesId]
                        else cachedBooleanValues[columnIndex][currentRowId]
                    }

                    override fun getNumeric(columnIndex: Int): Double {
                        return if (isKeyColumn[columnIndex]) cachedNumericValues[columnIndex][timeSeriesId]
                        else cachedNumericValues[columnIndex][currentRowId]
                    }

                    override fun getString(columnIndex: Int): String {
                        return if (isKeyColumn[columnIndex]) cachedStringValues[columnIndex][timeSeriesId]!!
                        else cachedStringValues[columnIndex][currentRowId]!!
                    }
                }

                override fun loadNext(): Boolean {
                    if (currentRowId == lastRowId) return false
                    currentRowId++
                    return true
                }
            }
        }

        override fun loadNext(): Boolean {
            // Return false if all cache time series have been read
            if (currentTimeSeriesId + 1 >= cachedTimeSeriesCount) return false
            // Move to the first row of the next time series
            if (currentTimeSeriesId == -1) {
                currentTimeSeriesFirstRow = 0
            } else {
                currentTimeSeriesFirstRow += rowsPerTimeSeries[currentTimeSeriesId]
            }
            // Increment the current time series ID
            currentTimeSeriesId++
            return true
        }
    }

    fun clear() {
        cachedRowCount = 0
        cachedTimeSeriesCount = 0
    }

    fun finalize() {
        clear()
        maxCachedRowCount = 0
        maxCachedTimeSeriesCount = 0
        Arrays.fill(cachedBooleanValues, booleanArrayOf())
        Arrays.fill(cachedNumericValues, doubleArrayOf())
        Arrays.fill(cachedStringValues, emptyArray<String?>())
        rowsPerTimeSeries = intArrayOf()
    }

    private fun expandTimeSeriesCache() {
        val newSize = maxCachedTimeSeriesCount * 2

        // Expand key column cache
        for (c in keyColumns) {
            when (columnTypes[c]) {
                IntTypes.TYPE_BOOLEAN -> cachedBooleanValues[c] = cachedBooleanValues[c].copyOf(newSize)
                IntTypes.TYPE_NUMERIC -> cachedNumericValues[c] = cachedNumericValues[c].copyOf(newSize)
                IntTypes.TYPE_STRING -> cachedStringValues[c] = cachedStringValues[c].copyOf(newSize)
            }
        }
        // Expand row count cache
        rowsPerTimeSeries = rowsPerTimeSeries.copyOf(newSize)

        maxCachedTimeSeriesCount = newSize
    }

    private fun expandRowCache() {
        val newSize = maxCachedRowCount * 2

        // Expand value column cache
        for (c in valueColumns) {
            when (columnTypes[c]) {
                IntTypes.TYPE_BOOLEAN -> cachedBooleanValues[c] = cachedBooleanValues[c].copyOf(newSize)
                IntTypes.TYPE_NUMERIC -> cachedNumericValues[c] = cachedNumericValues[c].copyOf(newSize)
                IntTypes.TYPE_STRING -> cachedStringValues[c] = cachedStringValues[c].copyOf(newSize)
            }
        }

        maxCachedRowCount = newSize
    }

    companion object {
        private const val INITIAL_CACHE_SIZE = 16
    }
}