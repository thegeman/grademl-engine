package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.TimeSeriesIterator

object TablePrinter {

    fun print(
        timeSeriesIterator: TimeSeriesIterator,
        maxLines: Int? = 200,
        output: StringBuilder? = null
    ) {
        fun localPrint(s: String) = output?.append(s) ?: print(s)
        fun localPrintln(s: String) = output?.appendLine(s) ?: println(s)

        require(maxLines == null || maxLines > 0)

        val showFirst = if (maxLines != null) (maxLines - 1) / 2 + 1 else Int.MAX_VALUE
        val showLast = if (maxLines != null) maxLines / 2 else 0
        val header = listOf("ROW", "TS") + timeSeriesIterator.schema.columns.map { it.identifier }
        val lines = ArrayList<Array<String>>()
        var lineCount = 0
        var lineIndex = 0
        var timeSeriesCount = 0
        while (timeSeriesIterator.loadNext()) {
            timeSeriesCount++

            val rowIter = timeSeriesIterator.currentTimeSeries.rowIterator()
            while (rowIter.loadNext()) {
                val row = rowIter.currentRow
                lineCount++
                if (lineIndex >= lines.size) {
                    lines.add(Array(timeSeriesIterator.schema.columns.size + 2) { "" })
                }

                lines[lineIndex][0] = lineCount.toString()
                lines[lineIndex][1] = timeSeriesCount.toString()
                for (c in rowIter.schema.columns.withIndex()) {
                    val value = when (c.value.type) {
                        Type.UNDEFINED -> "UNDEFINED"
                        Type.BOOLEAN -> row.getBoolean(c.index).toString()
                        Type.NUMERIC -> row.getNumeric(c.index).toString()
                        Type.STRING -> row.getString(c.index)
                    }
                    lines[lineIndex][c.index + 2] = value
                }

                lineIndex++
                if (lineIndex >= showFirst + showLast) {
                    lineIndex = showFirst
                }
            }
        }

        val startLines = when {
            lineCount <= showFirst + showLast -> lines.slice(0 until lineCount)
            showFirst > 0 -> lines.slice(0 until showFirst)
            else -> null
        }

        val endLines = if (lineCount > showFirst + showLast && showLast > 0) {
            lines.slice(lineIndex until lines.size) + lines.slice(showFirst until lineIndex)
        } else {
            null
        }

        val columnWidths = header.indices.map { c ->
            val maxValueWidth = if (lineCount == 0) 0 else
                (0 until minOf(lineCount, lines.size)).maxOf { lines[it][c].length }
            maxOf(header[c].length, maxValueWidth)
        }

        localPrintln("-".repeat(columnWidths.sum() + 3 * header.size + 1))
        for (c in header.indices) {
            localPrint("| ${header[c].padEnd(columnWidths[c])} ")
        }
        localPrintln("|")
        if (startLines != null) {
            localPrintln("-".repeat(columnWidths.sum() + 3 * header.size + 1))
            for (l in startLines) {
                for (c in l.indices) {
                    localPrint("| ${l[c].padEnd(columnWidths[c])} ")
                }
                localPrintln("|")
            }
        }

        if (lineCount > lines.size) {
            localPrintln("~".repeat(columnWidths.sum() + 3 * header.size + 1))
        } else if (lineCount > 0) {
            localPrintln("-".repeat(columnWidths.sum() + 3 * header.size + 1))
        }

        if (endLines != null) {
            for (l in endLines) {
                for (c in l.indices) {
                    localPrint("| ${l[c].padEnd(columnWidths[c])} ")
                }
                localPrintln("|")
            }
            localPrintln("-".repeat(columnWidths.sum() + 3 * header.size + 1))
        }

        localPrintln("Showing ${minOf(lineCount, lines.size)} of $lineCount rows.")
    }

}