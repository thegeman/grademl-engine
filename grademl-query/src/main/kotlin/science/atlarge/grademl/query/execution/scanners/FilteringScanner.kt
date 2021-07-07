package science.atlarge.grademl.query.execution.scanners

import science.atlarge.grademl.query.model.Row
import science.atlarge.grademl.query.model.RowScanner
import science.atlarge.grademl.query.nextOrNull

class FilteringScanner(
    private val baseScanner: RowScanner,
    private val filter: (Row) -> Boolean
) : RowScanner() {

    override fun fetchRow(): Row? {
        while (true) {
            val inputRow = baseScanner.nextOrNull() ?: return null
            if (filter(inputRow)) return inputRow
        }
    }

}