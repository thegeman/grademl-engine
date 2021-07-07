package science.atlarge.grademl.query.execution.impl

import science.atlarge.grademl.query.model.Row
import science.atlarge.grademl.query.model.RowScanner
import science.atlarge.grademl.query.model.TypedValue
import java.util.*

class TemporalJoinScanner(
    leftInput: TemporalJoinScannerInput,
    rightInput: TemporalJoinScannerInput
) : RowScanner() {

    // Row output format:
    // - 2 columns for start_time and end_time
    // - K or K - 2 columns from left table (depending on leftInput.dropTimeColumns)
    // - L or L - 2 columns from left table (depending on rightInput.dropTimeColumns)

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
    private var leftRowCache = LinkedList<Array<TypedValue>>()
    private var rightRowCache = LinkedList<Array<TypedValue>>()
    private var latestRowIsFromLeft = false
    private var latestRowStartTime = 0.0
    private var latestRowEndTime = 0.0
    private var opposingCacheIterator: MutableIterator<Array<TypedValue>>? = null
    private val scratchValue = TypedValue()

    // Output
    private val outputWrapper = RowWrapper()

    override fun fetchRow(): Row? {
        // Look for a row to return until a row is found or the end of the input is reached
        while (true) {
            // If no row is currently active, pick the next left-hand or right-hand row, whichever starts first
            if (opposingCacheIterator == null) {
                // Look ahead at both inputs
                val nextLeftRow = leftOrderedInputScanner.peek() ?: return null
                val nextRightRow = rightOrderedInputScanner.peek() ?: return null
                // Pick the one that starts first
                val leftStartTime = nextLeftRow.readValue(leftStartTimeColumnIndex, scratchValue).numericValue
                val rightStartTime = nextRightRow.readValue(rightStartTimeColumnIndex, scratchValue).numericValue
                if (leftStartTime <= rightStartTime) {
                    addLeftRowToCache(nextLeftRow)
                    latestRowIsFromLeft = true
                    latestRowStartTime = leftStartTime
                    latestRowEndTime = nextLeftRow.readValue(leftEndTimeColumnIndex, scratchValue).numericValue
                    opposingCacheIterator = rightRowCache.iterator()
                    leftOrderedInputScanner.next()
                } else {
                    addRightRowToCache(nextRightRow)
                    latestRowIsFromLeft = false
                    latestRowStartTime = rightStartTime
                    latestRowEndTime = nextRightRow.readValue(rightEndTimeColumnIndex, scratchValue).numericValue
                    opposingCacheIterator = leftRowCache.iterator()
                    rightOrderedInputScanner.next()
                }
            }

            // Find the next row from the opposing input table that overlaps with the latest selected row
            val opposingEndTimeColumnIndex = if (latestRowIsFromLeft) rightEndTimeColumnIndex else
                leftEndTimeColumnIndex
            val opposingRow = findCachedRowEndingAfter(
                targetTime = latestRowStartTime,
                endTimeColumnIndex = opposingEndTimeColumnIndex
            )
            // If no overlapping row is found, fetch a new row from the input
            if (opposingRow == null) {
                opposingCacheIterator = null
                continue
            }

            // Return a joined row
            val opposingRowEndTime = opposingRow[opposingEndTimeColumnIndex].numericValue
            outputWrapper.startTime = latestRowStartTime
            outputWrapper.endTime = minOf(latestRowEndTime, opposingRowEndTime)
            if (latestRowIsFromLeft) {
                outputWrapper.leftRow = leftRowCache.last
                outputWrapper.rightRow = opposingRow
            } else {
                outputWrapper.leftRow = opposingRow
                outputWrapper.rightRow = rightRowCache.last
            }
            return outputWrapper
        }
    }

    private fun addLeftRowToCache(row: Row) {
        val rowValues = Array(leftColumnCount) { TypedValue() }
        for (i in 0 until leftColumnCount) {
            row.readValue(i, rowValues[i])
        }
        leftRowCache.add(rowValues)
    }

    private fun addRightRowToCache(row: Row) {
        val rowValues = Array(rightColumnCount) { TypedValue() }
        for (i in 0 until rightColumnCount) {
            row.readValue(i, rowValues[i])
        }
        rightRowCache.add(rowValues)
    }

    private fun findCachedRowEndingAfter(targetTime: Double, endTimeColumnIndex: Int): Array<TypedValue>? {
        // Iterate through the currently "opposing" cache, deleting stale entries along the way
        val iterator = opposingCacheIterator!!
        while (iterator.hasNext()) {
            val nextRow = iterator.next()
            // Check the end time of the next row
            val endTime = nextRow[endTimeColumnIndex].numericValue
            if (endTime <= targetTime) iterator.remove()
            else return nextRow
        }
        return null
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
        lateinit var leftRow: Array<TypedValue>
        lateinit var rightRow: Array<TypedValue>

        override val columnCount = rightInputOffset + rightColumnMapping.size

        override fun readValue(columnId: Int, outValue: TypedValue): TypedValue {
            when {
                columnId == 0 -> outValue.numericValue = startTime
                columnId == 1 -> outValue.numericValue = endTime
                columnId < rightInputOffset -> leftRow[leftColumnMapping[columnId - leftInputOffset]].copyTo(outValue)
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