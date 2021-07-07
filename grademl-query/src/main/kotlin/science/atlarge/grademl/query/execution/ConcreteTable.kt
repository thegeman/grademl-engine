package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.ensureExhaustive
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.*

class ConcreteTable private constructor(
    override val columns: List<Column>,
    private val booleanColumns: Array<BooleanArray>,
    private val numericColumns: Array<DoubleArray>,
    private val stringColumns: Array<Array<String>>
) : Table {

    val rowCount =
        booleanColumns.find { it.isNotEmpty() }?.size ?: numericColumns.find { it.isNotEmpty() }?.size
        ?: stringColumns.find { it.isNotEmpty() }?.size ?: 0

    override fun scan() = object : RowScanner() {

        private var nextRowNumber = 0

        private val rowWrapper = object : Row {

            var rowNumber = 0

            override val columnCount = columns.size

            override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
                when (columns[columnId].type) {
                    Type.UNDEFINED -> outValue.clear()
                    Type.BOOLEAN -> outValue.booleanValue = booleanColumns[columnId][rowNumber]
                    Type.NUMERIC -> outValue.numericValue = numericColumns[columnId][rowNumber]
                    Type.STRING -> outValue.stringValue = stringColumns[columnId][rowNumber]
                }.ensureExhaustive
                return outValue
            }

        }

        override fun fetchRow(): Row? {
            if (nextRowNumber >= rowCount) return null
            rowWrapper.rowNumber = nextRowNumber
            nextRowNumber++
            return rowWrapper
        }

    }

    companion object {

        @Suppress("UNCHECKED_CAST")
        fun from(table: Table): ConcreteTable {
            // Create a copy of the column list
            val columns = table.columns.toList()

            // Construct data arrays for each column
            var dataArraySize = 16
            val booleanColumns = Array(columns.size) { columnId ->
                if (columns[columnId].type == Type.BOOLEAN) BooleanArray(dataArraySize)
                else booleanArrayOf()
            }
            val numericColumns = Array(columns.size) { columnId ->
                if (columns[columnId].type == Type.NUMERIC) DoubleArray(dataArraySize)
                else doubleArrayOf()
            }
            val stringColumns = Array(columns.size) { columnId ->
                if (columns[columnId].type == Type.STRING) arrayOfNulls<String>(dataArraySize)
                else emptyArray()
            }

            // Read every row from the input table and add it to the data arrays
            var rowsAdded = 0
            val scratch = TypedValue()
            for (row in table.scan()) {
                // Extend the data arrays if needed
                if (rowsAdded >= dataArraySize) {
                    dataArraySize *= 2
                    for (c in columns.indices) {
                        when (columns[c].type) {
                            Type.UNDEFINED -> continue
                            Type.BOOLEAN -> booleanColumns[c] = booleanColumns[c].copyOf(dataArraySize)
                            Type.NUMERIC -> numericColumns[c] = numericColumns[c].copyOf(dataArraySize)
                            Type.STRING -> stringColumns[c] = stringColumns[c].copyOf(dataArraySize)
                        }.ensureExhaustive
                    }
                }

                // Add the next row
                for (c in columns.indices) {
                    row.readValue(c, scratch)
                    when (columns[c].type) {
                        Type.UNDEFINED -> continue
                        Type.BOOLEAN -> booleanColumns[c][rowsAdded] = scratch.booleanValue
                        Type.NUMERIC -> numericColumns[c][rowsAdded] = scratch.numericValue
                        Type.STRING -> stringColumns[c][rowsAdded] = scratch.stringValue
                    }.ensureExhaustive
                }

                rowsAdded++
            }

            // Trim the data arrays to the correct size
            for (c in columns.indices) {
                when (columns[c].type) {
                    Type.UNDEFINED -> continue
                    Type.BOOLEAN -> booleanColumns[c] = booleanColumns[c].copyOf(rowsAdded)
                    Type.NUMERIC -> numericColumns[c] = numericColumns[c].copyOf(rowsAdded)
                    Type.STRING -> stringColumns[c] = stringColumns[c].copyOf(rowsAdded)
                }.ensureExhaustive
            }

            // Create the ConcreteTable
            return ConcreteTable(columns, booleanColumns, numericColumns, stringColumns as Array<Array<String>>)
        }

    }

}