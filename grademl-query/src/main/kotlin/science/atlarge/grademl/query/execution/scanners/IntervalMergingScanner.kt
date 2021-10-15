package science.atlarge.grademl.query.execution.scanners

import science.atlarge.grademl.query.model.Row
import science.atlarge.grademl.query.model.RowScanner
import science.atlarge.grademl.query.model.TypedValue

class IntervalMergingScanner(
    private val baseScanner: RowScanner,
    private val columnCount: Int,
    private val startTimeColumn: Int,
    private val endTimeColumn: Int
) : RowScanner() {

    private val prefetchedValues = Array(columnCount) { TypedValue() }
    private val rowWrapper = object : Row {
        override val columnCount: Int
            get() = this@IntervalMergingScanner.columnCount
        override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
            prefetchedValues[columnId].copyTo(outValue)
            return outValue
        }
    }

    private val scratch = TypedValue()

    private var inputRowCount = 0L
    private var outputRowCount = 0L

    override fun fetchRow(): Row? {
        // Stop if there are no more input rows
        if (!baseScanner.hasNext()) {
            println("[DEBUG] End of interval merge scanner. Input rows: $inputRowCount. Output rows: $outputRowCount.")
            return null
        }
        // Fetch the next row
        val inputRow = baseScanner.next()
        inputRowCount++
        copyInputRow(inputRow)
        // Look ahead at additional rows to see if they can be merged
        while (true) {
            // Peek at the next row
            val nextRow = baseScanner.peek() ?: break
            // Check all value columns for equality
            if (!hasIdenticalValues(nextRow)) break
            // Check that the timestamps can be merged
            if (!hasConsecutiveTimestamp(nextRow)) break
            // Merge the intervals
            updateEndTimestamp(nextRow)
            baseScanner.next()
            inputRowCount++
        }
        outputRowCount++
        return rowWrapper
    }

    private fun copyInputRow(row: Row) {
        for (i in 0 until columnCount) {
            row.readValue(i, prefetchedValues[i])
        }
    }

    private fun hasIdenticalValues(row: Row): Boolean {
        for (i in 0 until columnCount) {
            // Skip timestamp columns
            if (i == startTimeColumn || i == endTimeColumn) continue
            // Read the next column
            row.readValue(i, scratch)
            // Compare against the stored row's value
            if (scratch != prefetchedValues[i]) return false
        }
        return true
    }

    private fun hasConsecutiveTimestamp(row: Row): Boolean {
        // Read start time of next row
        row.readValue(startTimeColumn, scratch)
        // Compare against current end time
        return scratch == prefetchedValues[endTimeColumn]
    }

    private fun updateEndTimestamp(row: Row) {
        row.readValue(endTimeColumn, prefetchedValues[endTimeColumn])
    }

}