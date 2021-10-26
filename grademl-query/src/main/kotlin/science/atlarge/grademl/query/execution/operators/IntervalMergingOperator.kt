package science.atlarge.grademl.query.execution.operators

import science.atlarge.grademl.query.execution.IntTypes
import science.atlarge.grademl.query.execution.IntTypes.toInt
import science.atlarge.grademl.query.model.*

class IntervalMergingOperator(
    private val input: QueryOperator
) : QueryOperator {

    private val hasDurationColumn = input.schema.indexOfColumn(Columns.DURATION) == Columns.INDEX_DURATION

    override val schema: TableSchema
        get() = input.schema

    init {
        require(input.schema.indexOfColumn(Columns.START_TIME) == Columns.INDEX_START_TIME) {
            "Input to IntervalMergingOperator mush have start time column at index ${Columns.INDEX_START_TIME}"
        }
        require(input.schema.indexOfColumn(Columns.END_TIME) == Columns.INDEX_END_TIME) {
            "Input to IntervalMergingOperator mush have end time column at index ${Columns.INDEX_END_TIME}"
        }
    }

    override fun execute() = object : TimeSeriesIterator {
        private val timeSeriesIterator = input.execute()
        private val columnTypes = input.schema.columns.map { it.type.toInt() }.toIntArray()
        private val nonReservedValueColumns = input.schema.columns.mapIndexedNotNull { index, column ->
            if (!column.isKey && column.identifier !in Columns.RESERVED_COLUMN_NAMES) index else null
        }

        override val schema: TableSchema
            get() = input.schema
        override val currentTimeSeries = object : TimeSeries {
            override val schema: TableSchema
                get() = input.schema

            override fun getBoolean(columnIndex: Int) = timeSeriesIterator.currentTimeSeries.getBoolean(columnIndex)
            override fun getNumeric(columnIndex: Int) = timeSeriesIterator.currentTimeSeries.getNumeric(columnIndex)
            override fun getString(columnIndex: Int) = timeSeriesIterator.currentTimeSeries.getString(columnIndex)

            override fun rowIterator() = object : RowIterator {
                private val rowIterator = timeSeriesIterator.currentTimeSeries.rowIterator()
                private var peekedAtRow = false
                private val booleanValues = BooleanArray(columnTypes.size)
                private val numericValues = DoubleArray(columnTypes.size)
                private val stringValues = arrayOfNulls<String>(columnTypes.size)

                override val schema: TableSchema
                    get() = input.schema
                override val currentRow = object : Row {
                    override val schema: TableSchema
                        get() = input.schema

                    override fun getBoolean(columnIndex: Int) = booleanValues[columnIndex]
                    override fun getNumeric(columnIndex: Int) = numericValues[columnIndex]
                    override fun getString(columnIndex: Int) = stringValues[columnIndex]!!
                }

                override fun loadNext(): Boolean {
                    // Read the next row
                    if (!peekedAtRow && !rowIterator.loadNext()) return false
                    peekedAtRow = false
                    // Cache its values
                    val initialRow = rowIterator.currentRow
                    for (c in columnTypes.indices) {
                        when (columnTypes[c]) {
                            IntTypes.TYPE_BOOLEAN -> booleanValues[c] = initialRow.getBoolean(c)
                            IntTypes.TYPE_NUMERIC -> numericValues[c] = initialRow.getNumeric(c)
                            IntTypes.TYPE_STRING -> stringValues[c] = initialRow.getString(c)
                            else -> throw IllegalArgumentException("Unsupported type")
                        }
                    }
                    // Read rows as long as their values are identical to merge them
                    while (rowIterator.loadNext()) {
                        // Compare the next row against the cached row
                        val nextRow = rowIterator.currentRow
                        // Check if the timestamps are consecutive
                        if (numericValues[Columns.INDEX_END_TIME] != nextRow.getNumeric(Columns.INDEX_START_TIME)) {
                            peekedAtRow = true
                            break
                        }
                        // Check all (regular) value columns
                        var isEqual = true
                        for (c in nonReservedValueColumns) {
                            isEqual = when (columnTypes[c]) {
                                IntTypes.TYPE_BOOLEAN -> booleanValues[c] == nextRow.getBoolean(c)
                                IntTypes.TYPE_NUMERIC -> numericValues[c] == nextRow.getNumeric(c)
                                IntTypes.TYPE_STRING -> stringValues[c] == nextRow.getString(c)
                                else -> throw IllegalArgumentException("Unsupported type")
                            }
                            if (!isEqual) break
                        }
                        // If any columns are different, do not merge
                        if (!isEqual) {
                            peekedAtRow = true
                            break
                        }
                        // Combine the rows by merging time intervals
                        numericValues[Columns.INDEX_END_TIME] = nextRow.getNumeric(Columns.INDEX_END_TIME)
                    }
                    // Set the merged row's duration (if applicable)
                    if (hasDurationColumn) {
                        numericValues[Columns.INDEX_DURATION] = numericValues[Columns.INDEX_END_TIME] -
                                numericValues[Columns.INDEX_START_TIME]
                    }
                    return true
                }
            }
        }

        override fun loadNext() = timeSeriesIterator.loadNext()
    }

}