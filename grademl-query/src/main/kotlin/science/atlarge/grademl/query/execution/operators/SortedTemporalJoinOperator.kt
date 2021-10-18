package science.atlarge.grademl.query.execution.operators

import science.atlarge.grademl.query.execution.IndexedSortColumn
import science.atlarge.grademl.query.execution.operators.IntTypes.TYPE_BOOLEAN
import science.atlarge.grademl.query.execution.operators.IntTypes.TYPE_NUMERIC
import science.atlarge.grademl.query.execution.operators.IntTypes.TYPE_STRING
import science.atlarge.grademl.query.execution.operators.IntTypes.toInt
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.v2.*

class SortedTemporalJoinOperator(
    private val leftInput: QueryOperator,
    private val rightInput: QueryOperator,
    override val schema: TableSchema,
    leftJoinColumns: List<IndexedSortColumn>,
    rightJoinColumns: List<IndexedSortColumn>
) : QueryOperator {

    private val joinColumnTypes = leftJoinColumns.map {
        leftInput.schema.columns[it.columnIndex].type
    }.toTypedArray()
    private val joinColumnAscending = leftJoinColumns.map { it.ascending }.toBooleanArray()
    private val leftJoinColumnIndices = leftJoinColumns.map { it.columnIndex }.toIntArray()
    private val rightJoinColumnIndices = rightJoinColumns.map { it.columnIndex }.toIntArray()

    init {
        require(rightJoinColumns.size == leftJoinColumns.size) {
            "Must have the same number of left and right join columns"
        }
        rightJoinColumns.forEachIndexed { i, rc ->
            require(rightInput.schema.columns[rc.columnIndex].type == joinColumnTypes[i]) {
                "Join columns must have the same type"
            }
            require(rc.ascending == joinColumnAscending[i]) {
                "Join columns must have the sort direction"
            }
        }
        require(schema.columns[0] == Columns.START_TIME) { "Temporal join must produce _start_time column" }
        require(schema.columns[1] == Columns.END_TIME) { "Temporal join must produce _end_time column" }
        require(schema.columns[2] == Columns.DURATION) { "Temporal join must produce _duration column" }
    }

    override fun execute(): TimeSeriesIterator = TemporalJoinTimeSeriesIterator(
        schema = schema,
        leftInput = leftInput.execute(),
        rightInput = rightInput.execute(),
        joinColumnTypes = joinColumnTypes,
        joinColumnAscending = joinColumnAscending,
        leftJoinColumnIndices = leftJoinColumnIndices,
        rightJoinColumnIndices = rightJoinColumnIndices
    )

}

private class TemporalJoinTimeSeriesIterator(
    override val schema: TableSchema,
    private val leftInput: TimeSeriesIterator,
    private val rightInput: TimeSeriesIterator,
    joinColumnTypes: Array<Type>,
    private val joinColumnAscending: BooleanArray,
    private val leftJoinColumnIndices: IntArray,
    private val rightJoinColumnIndices: IntArray
) : TimeSeriesIterator {

    // Store column types as integers for faster look-ups
    private val joinColumnTypes = joinColumnTypes.map { it.toInt() }.toIntArray()

    // Cache the values of join columns for quick comparison with other time series
    private val cachedBooleanJoinColumnValue = BooleanArray(joinColumnTypes.size)
    private val cachedNumericJoinColumnValue = DoubleArray(joinColumnTypes.size)
    private val cachedStringJoinColumnValue = arrayOfNulls<String?>(joinColumnTypes.size)

    // Cache time series from the left input
    private val leftTimeSeriesCache = TimeSeriesCache(leftInput.schema)
    private var leftCacheIterator: TimeSeriesIterator? = null

    // Track whether we have peeked at the input iterators
    private var leftInputValid = false
    private var rightInputValid = false

    // Create a comparator that determines if two time series can be joined
    private val comparator = object : Comparator<TimeSeries> {
        private val types = this@TemporalJoinTimeSeriesIterator.joinColumnTypes
        private val columnDirectionMultipliers = IntArray(joinColumnAscending.size) {
            if (joinColumnAscending[it]) 1 else -1
        }

        override fun compare(left: TimeSeries, right: TimeSeries): Int {
            for (c in types.indices) {
                val lc = leftJoinColumnIndices[c]
                val rc = rightJoinColumnIndices[c]
                val result = when (types[c]) {
                    TYPE_BOOLEAN -> left.getBoolean(lc).compareTo(right.getBoolean(rc))
                    TYPE_NUMERIC -> left.getNumeric(lc).compareTo(right.getNumeric(rc))
                    TYPE_STRING -> left.getString(lc).compareTo(right.getString(rc))
                    else -> throw IllegalStateException()
                }
                if (result != 0) return result * columnDirectionMultipliers[c]
            }
            return 0
        }
    }

    private val temporalJoinTimeSeries = TemporalJoinTimeSeries(schema, leftInput.schema.columns.size)
    override val currentTimeSeries: TimeSeries
        get() = temporalJoinTimeSeries

    override fun loadNext(): Boolean {
        // Join the next time series from the cache with the current right input
        if (leftCacheIterator != null && rightInputValid) {
            if (leftCacheIterator!!.loadNext()) {
                temporalJoinTimeSeries.setInputs(leftCacheIterator!!.currentTimeSeries, rightInput.currentTimeSeries)
                return true
            } else {
                leftCacheIterator = null
            }
        }
        // Check if the next right input time series can be joined with the left input cache
        if (leftTimeSeriesCache.numCachedTimeSeries > 0 && isNextRightTimeSeriesMatching()) {
            // Restart the left cache iterator to be joined with the new right input
            leftCacheIterator = leftTimeSeriesCache.iterator()
            if (!leftCacheIterator!!.loadNext()) throw IllegalStateException("Cache iterator is empty after filling the cache")
            temporalJoinTimeSeries.setInputs(leftCacheIterator!!.currentTimeSeries, rightInput.currentTimeSeries)
            return true
        } else if (!rightInputValid) {
            // Abort if no right input was loaded
            return false
        }
        // Clear the cache to load a new group of time series
        leftTimeSeriesCache.clear()
        // Find matching time series from the left and right input to join
        if (!findMatchingTimeSeries()) return false
        // Load all matching time series from the left input into the cache
        leftTimeSeriesCache.addTimeSeries(leftInput.currentTimeSeries)
        leftInputValid = false
        val rightTimeSeries = rightInput.currentTimeSeries
        while (true) {
            // Read a time series from the left input
            if (!leftInput.loadNext()) break
            val leftTimeSeries = leftInput.currentTimeSeries
            // Check if it can be joined with the current right time series
            if (comparator.compare(leftTimeSeries, rightTimeSeries) == 0) {
                leftTimeSeriesCache.addTimeSeries(leftTimeSeries)
            } else {
                // If not, mark that we have peeked at the next left input and stop
                leftInputValid = true
                break
            }
        }
        // Create a new iterator over the cached inputs
        leftCacheIterator = leftTimeSeriesCache.iterator()
        // Cache the join column values from our right input
        for (c in rightJoinColumnIndices.indices) {
            when (joinColumnTypes[c]) {
                TYPE_BOOLEAN -> cachedBooleanJoinColumnValue[c] = rightTimeSeries.getBoolean(rightJoinColumnIndices[c])
                TYPE_NUMERIC -> cachedNumericJoinColumnValue[c] = rightTimeSeries.getNumeric(rightJoinColumnIndices[c])
                TYPE_STRING -> cachedStringJoinColumnValue[c] = rightTimeSeries.getString(rightJoinColumnIndices[c])
            }
        }
        // Prepare the next joined time series
        if (!leftCacheIterator!!.loadNext()) throw IllegalStateException("Cache iterator is empty after filling the cache")
        temporalJoinTimeSeries.setInputs(leftCacheIterator!!.currentTimeSeries, rightTimeSeries)
        return true
    }

    private fun isNextRightTimeSeriesMatching(): Boolean {
        // Read the next right time series
        rightInputValid = false
        if (!rightInput.loadNext()) return false
        rightInputValid = true
        // Check if it matches the cached column values
        val rightTimeSeries = rightInput.currentTimeSeries
        for (c in rightJoinColumnIndices.indices) {
            when (joinColumnTypes[c]) {
                TYPE_BOOLEAN ->
                    if (cachedBooleanJoinColumnValue[c] != rightTimeSeries.getBoolean(rightJoinColumnIndices[c]))
                        return false
                TYPE_NUMERIC ->
                    if (cachedNumericJoinColumnValue[c] != rightTimeSeries.getNumeric(rightJoinColumnIndices[c]))
                        return false
                TYPE_STRING ->
                    if (cachedStringJoinColumnValue[c] != rightTimeSeries.getString(rightJoinColumnIndices[c]))
                        return false
            }
        }
        return true
    }

    private fun findMatchingTimeSeries(): Boolean {
        // Get the first time series from the left input
        if (!leftInputValid && !leftInput.loadNext()) return false
        leftInputValid = true
        var leftTimeSeries = leftInput.currentTimeSeries
        // Get the first time series from the right input
        if (!rightInputValid && !rightInput.loadNext()) return false
        rightInputValid = true
        var rightTimeSeries = rightInput.currentTimeSeries

        // Compare the left and right time series until a match is found
        while (true) {
            val comparison = comparator.compare(leftTimeSeries, rightTimeSeries)
            if (comparison == 0) return true
            // Check if left or right is "smaller"
            if (comparison < 0) {
                // Left is smaller, so load the next time series from the left input
                leftInputValid = false
                if (!leftInput.loadNext()) return false
                leftInputValid = true
                leftTimeSeries = leftInput.currentTimeSeries
            } else {
                // Right is smaller, so load the next time series from the right input
                rightInputValid = false
                if (!rightInput.loadNext()) return false
                rightInputValid = true
                rightTimeSeries = rightInput.currentTimeSeries
            }
        }
    }

}

private class TemporalJoinTimeSeries(
    override val schema: TableSchema,
    leftColumnCount: Int
) : TimeSeries {

    private val leftColumnOffset = Columns.INDEX_NOT_RESERVED
    private val rightColumnOffset = leftColumnOffset + leftColumnCount

    private lateinit var leftTimeSeries: TimeSeries
    private lateinit var rightTimeSeries: TimeSeries

    fun setInputs(leftTimeSeries: TimeSeries, rightTimeSeries: TimeSeries) {
        this.leftTimeSeries = leftTimeSeries
        this.rightTimeSeries = rightTimeSeries
    }

    override fun getBoolean(columnIndex: Int): Boolean {
        return when {
            columnIndex >= rightColumnOffset -> rightTimeSeries.getBoolean(columnIndex - rightColumnOffset)
            columnIndex >= leftColumnOffset -> leftTimeSeries.getBoolean(columnIndex - leftColumnOffset)
            else -> throw IllegalArgumentException("Column $columnIndex does not exist or is not a key column")
        }
    }

    override fun getNumeric(columnIndex: Int): Double {
        return when {
            columnIndex >= rightColumnOffset -> rightTimeSeries.getNumeric(columnIndex - rightColumnOffset)
            columnIndex >= leftColumnOffset -> leftTimeSeries.getNumeric(columnIndex - leftColumnOffset)
            else -> throw IllegalArgumentException("Column $columnIndex does not exist or is not a key column")
        }
    }

    override fun getString(columnIndex: Int): String {
        return when {
            columnIndex >= rightColumnOffset -> rightTimeSeries.getString(columnIndex - rightColumnOffset)
            columnIndex >= leftColumnOffset -> leftTimeSeries.getString(columnIndex - leftColumnOffset)
            else -> throw IllegalArgumentException("Column $columnIndex does not exist or is not a key column")
        }
    }

    override fun rowIterator(): RowIterator {
        val leftIterator = leftTimeSeries.rowIterator()
        val rightIterator = rightTimeSeries.rowIterator()
        return object : RowIterator {
            private var leftStart: Double = 0.0
            private var leftEnd: Double = 0.0
            private var rightStart: Double = 0.0
            private var rightEnd: Double = 0.0

            private var leftRow: Row? = null
            private var rightRow: Row? = null

            override val schema: TableSchema
                get() = this@TemporalJoinTimeSeries.schema
            override val currentRow: Row = object : Row {
                override val schema: TableSchema
                    get() = this@TemporalJoinTimeSeries.schema

                override fun getBoolean(columnIndex: Int): Boolean {
                    return when {
                        columnIndex >= rightColumnOffset ->
                            rightRow!!.getBoolean(columnIndex - rightColumnOffset)
                        columnIndex >= leftColumnOffset ->
                            leftRow!!.getBoolean(columnIndex - leftColumnOffset)
                        else -> throw IllegalArgumentException("Column $columnIndex does not exist or is not a BOOLEAN column")
                    }
                }

                override fun getNumeric(columnIndex: Int): Double {
                    return when {
                        columnIndex == 0 -> maxOf(leftStart, rightStart)
                        columnIndex == 1 -> minOf(leftEnd, rightEnd)
                        columnIndex == 2 -> minOf(leftEnd, rightEnd) - maxOf(leftStart, rightStart)
                        columnIndex >= rightColumnOffset ->
                            rightTimeSeries.getNumeric(columnIndex - rightColumnOffset)
                        columnIndex >= leftColumnOffset ->
                            leftTimeSeries.getNumeric(columnIndex - leftColumnOffset)
                        else -> throw IllegalArgumentException("Column $columnIndex does not exist or is not a NUMERIC column")
                    }
                }

                override fun getString(columnIndex: Int): String {
                    return when {
                        columnIndex >= rightColumnOffset -> rightTimeSeries.getString(columnIndex - rightColumnOffset)
                        columnIndex >= leftColumnOffset -> leftTimeSeries.getString(columnIndex - leftColumnOffset)
                        else -> throw IllegalArgumentException("Column $columnIndex does not exist or is not a STRING column")
                    }
                }
            }

            override fun loadNext(): Boolean {
                // Load new rows if either the left or right input is not cached
                val loadNewRows = leftRow == null || rightRow == null
                if (leftRow == null && !nextLeft()) return false
                if (rightRow == null && !nextRight()) return false
                // If both rows were cached, advance either left or right (whichever ends first)
                if (!loadNewRows) {
                    when {
                        leftEnd < rightEnd -> if (!nextLeft()) return false
                        leftEnd > rightEnd -> if (!nextRight()) return false
                        else -> if (!nextLeft() || !nextRight()) return false
                    }
                }
                // Keep loading new rows until overlap is found
                while (leftEnd <= rightStart || rightEnd <= leftStart) {
                    when {
                        leftEnd < rightEnd -> if (!nextLeft()) return false
                        leftEnd > rightEnd -> if (!nextRight()) return false
                    }
                }
                // We have found an overlapping pair of input rows
                return true
            }

            private fun nextLeft(): Boolean {
                leftRow = null
                if (!leftIterator.loadNext()) return false
                leftRow = leftIterator.currentRow
                leftStart = leftRow!!.getNumeric(0)
                leftEnd = leftRow!!.getNumeric(1)
                return true
            }

            private fun nextRight(): Boolean {
                rightRow = null
                if (!rightIterator.loadNext()) return false
                rightRow = rightIterator.currentRow
                rightStart = rightRow!!.getNumeric(0)
                rightEnd = rightRow!!.getNumeric(1)
                return true
            }
        }
    }
}