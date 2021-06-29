package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.Table
import science.atlarge.grademl.query.model.TypedValue

object TablePrinter {

    fun print(table: Table, showFirst: Int = 10, showLast: Int = 10) {
        val header = listOf("ROW") + table.columns.map { it.name }
        val lines = Array(showFirst + showLast) { Array(table.columns.size + 1) { "" } }
        var lineCount = 0
        var lineIndex = 0
        val sc = table.scan()
        val cellValue = TypedValue()
        for (row in sc) {
            lineCount++
            if (lineIndex >= lines.size) continue

            lines[lineIndex][0] = lineCount.toString()
            for (c in table.columns.indices) {
                row.readValue(c, cellValue)
                lines[lineIndex][c + 1] = cellValue.toString()
            }

            lineIndex++
            if (lineIndex == lines.size) {
                lineIndex = showFirst
            }
        }

        val startLines = when {
            lineCount <= lines.size -> lines.slice(0 until lineCount)
            showFirst > 0 -> lines.slice(0 until showFirst)
            else -> null
        }

        val endLines = if (lineCount > lines.size && showLast > 0) {
            lines.slice(lineIndex until lines.size) + lines.slice(showFirst until lineIndex)
        } else {
            null
        }

        val columnWidths = header.indices.map { c ->
            val maxValueWidth = if (lineCount == 0) 0 else
                    (0 until minOf(lineCount, lines.size)).maxOf { lines[it][c].length }
            maxOf(header[c].length, maxValueWidth)
        }

        println("-".repeat(columnWidths.sum() + 3 * header.size + 1))
        for (c in header.indices) {
            print("| ${header[c].padEnd(columnWidths[c])} ")
        }
        println("|")
        if (startLines != null) {
            println("-".repeat(columnWidths.sum() + 3 * header.size + 1))
            for (l in startLines) {
                for (c in l.indices) {
                    print("| ${l[c].padEnd(columnWidths[c])} ")
                }
                println("|")
            }
        }

        if (lineCount > lines.size) {
            println("~".repeat(columnWidths.sum() + 3 * header.size + 1))
        } else if (lineCount > 0) {
            println("-".repeat(columnWidths.sum() + 3 * header.size + 1))
        }

        if (endLines != null) {
            for (l in endLines) {
                for (c in l.indices) {
                    print("| ${l[c].padEnd(columnWidths[c])} ")
                }
                println("|")
            }
            println("-".repeat(columnWidths.sum() + 3 * header.size + 1))
        }

        println("Showing ${minOf(lineCount, lines.size)} of $lineCount rows.")
    }

}