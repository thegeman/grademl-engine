package science.atlarge.grademl.query.execution.impl

import science.atlarge.grademl.query.model.Row
import science.atlarge.grademl.query.model.RowScanner
import science.atlarge.grademl.query.model.TypedValue
import science.atlarge.grademl.query.nextOrNull
import java.util.*

class TemporalJoinScanner(
    leftInput: TemporalJoinScannerInput,
    rightInput: TemporalJoinScannerInput
) : RowScanner() {

    // Row output format:
    // - 2 columns for start_time and end_time
    // - K or K - 2 columns from left table (depending on dropLeftTimeColumns)
    // - L or L - 2 columns from left table (depending on dropLeftTimeColumns)
    
    // Extract inputs
    private val leftOrderedInputScanner = leftInput.orderedInputScanner
    private val leftColumnCount = leftInput.columnCount
    private val leftStartTimeColumnIndex = leftInput.startTimeColumnIndex
    private val leftEndTimeColumnIndex = leftInput.endTimeColumnIndex
    private val leftDropTimeColumns = leftInput.dropTimeColumns
    private val rightOrderedInputScanner = rightInput.orderedInputScanner
    private val rightColumnCount = rightInput.columnCount
    private val rightStartTimeColumnIndex = rightInput.startTimeColumnIndex
    private val rightEndTimeColumnIndex = rightInput.endTimeColumnIndex
    private val rightDropTimeColumns = rightInput.dropTimeColumns

    // Iteration state
    private var leftRow: Row? = null
    private var rightRowCache = LinkedList<Array<TypedValue>>()
    private var cacheIterator: MutableIterator<Array<TypedValue>>? = null
    private val scratchValue = TypedValue()

    // Output
    private val outputWrapper = RowWrapper()

    override fun fetchRow(): Row? {
        // Look for a row to return until a row is found or the end of the input is reached
        while (true) {
            // If no left-hand row is in the cache, fetch one and return if none exists
            if (leftRow == null) {
                leftRow = leftOrderedInputScanner.nextOrNull() ?: return null
                // Add additional rows from the right-hand table to the cache if applicable
                addOverlappingRowsToCache()
                // Reset the cache iterator
                cacheIterator = null
            }
            // Read the start and end time of the current left-hand row
            val leftStartTime = leftRow!!.readValue(leftStartTimeColumnIndex, scratchValue).numericValue
            val leftEndTime = leftRow!!.readValue(leftEndTimeColumnIndex, scratchValue).numericValue
            // Start an iterator on the right-hand row cache if needed
            if (cacheIterator == null) cacheIterator = rightRowCache.iterator()
            // Go through the row cache iterator
            val iterator = cacheIterator!!
            while (iterator.hasNext()) {
                // Read the next row
                val rightRow = iterator.next()
                // Read the start and end time of the current right-hand row
                val rightStartTime = rightRow[rightStartTimeColumnIndex].numericValue
                val rightEndTime = rightRow[rightEndTimeColumnIndex].numericValue
                // Drop rows with small enough end times that they cannot overlap with any future row
                if (rightEndTime <= leftStartTime) {
                    iterator.remove()
                    continue
                }
                // Return a joined row if the left- and right-hand rows overlap
                if (leftStartTime < rightEndTime && rightStartTime < leftEndTime) {
                    outputWrapper.startTime = maxOf(leftStartTime, rightStartTime)
                    outputWrapper.endTime = minOf(leftEndTime, rightEndTime)
                    outputWrapper.leftRow = leftRow!!
                    outputWrapper.rightRow = rightRow
                    return outputWrapper
                }
            }
            // Upon reaching the end of the right-hand row cache, move to the next left-hand row
            leftRow = null
        }
    }

    private fun addOverlappingRowsToCache() {
        // Read the end time of the current left-hand row
        val leftEndTime = leftRow!!.readValue(leftEndTimeColumnIndex, scratchValue).numericValue
        // Add right-hand row to the cache iff their start time is smaller than the end time of the left-hand row
        while (true) {
            // Check the start time of the next right-hand row
            val rightRow = rightOrderedInputScanner.peek() ?: break
            val rightStartTime = rightRow.readValue(rightStartTimeColumnIndex, scratchValue).numericValue
            if (rightStartTime >= leftEndTime) break
            // Add the row to the cache
            val rightRowValues = Array(rightColumnCount) { TypedValue() }
            for (i in 0 until rightColumnCount) {
                rightRow.readValue(i, rightRowValues[i])
            }
            rightRowCache.add(rightRowValues)
            // Move on to the next row
            rightOrderedInputScanner.next()
        }
    }

    private fun isRightRowTooOld(rightRow: Array<TypedValue>): Boolean {
        // Read the start time of the current left-hand row
        val leftStartTime = leftRow!!.readValue(leftStartTimeColumnIndex, scratchValue).numericValue
        // Read the end time of the current right-hand row
        val rightEndTime = rightRow[rightEndTimeColumnIndex].numericValue
        // Determine if the right row is old enough to be dropped
        return leftStartTime >= rightEndTime
    }

    private fun overlapsWithLeftRow(rightRow: Array<TypedValue>): Boolean {
        // Read the start and end time of the current left-hand row
        val leftStartTime = leftRow!!.readValue(leftStartTimeColumnIndex, scratchValue).numericValue
        val leftEndTime = leftRow!!.readValue(leftEndTimeColumnIndex, scratchValue).numericValue
        // Read the start and end time of the current right-hand row
        val rightStartTime = rightRow[rightStartTimeColumnIndex].numericValue
        val rightEndTime = rightRow[rightEndTimeColumnIndex].numericValue
        // Determine if the rows overlap
        return leftStartTime < rightEndTime && rightStartTime < leftEndTime
    }

    private inner class RowWrapper : Row {

        private val leftColumnMapping = (0 until leftColumnCount).run {
            if (leftDropTimeColumns) filter { it != leftStartTimeColumnIndex && it != leftEndTimeColumnIndex }
            else toList()
        }
        private val rightColumnMapping = (0 until rightColumnCount).run {
            if (rightDropTimeColumns) filter { it != rightStartTimeColumnIndex && it != rightEndTimeColumnIndex }
            else toList()
        }

        private val leftInputOffset = 2
        private val rightInputOffset = leftInputOffset + leftColumnMapping.size

        var startTime = 0.0
        var endTime = 0.0
        lateinit var leftRow: Row
        lateinit var rightRow: Array<TypedValue>

        override val columnCount = rightInputOffset + rightColumnMapping.size

        override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
            when {
                columnId == 0 -> outValue.numericValue = startTime
                columnId == 1 -> outValue.numericValue = endTime
                columnId < rightInputOffset ->
                    leftRow.readValue(leftColumnMapping[columnId - leftInputOffset], outValue)
                else -> rightRow[rightColumnMapping[columnId - rightInputOffset]].copyTo(outValue)
            }
            return outValue
        }

    }

}

class TemporalJoinScannerInput(
    val orderedInputScanner: RowScanner,
    val columnCount: Int,
    val startTimeColumnIndex: Int,
    val endTimeColumnIndex: Int,
    val dropTimeColumns: Boolean
)