package science.atlarge.grademl.query.execution.scanners

import science.atlarge.grademl.query.model.Row
import science.atlarge.grademl.query.model.RowScanner
import science.atlarge.grademl.query.nextOrNull

class LimitScanner(
    private val baseScanner: RowScanner,
    private val limit: Int
) : RowScanner() {

    private var rowsReturned = 0

    override fun fetchRow(): Row? {
        if (rowsReturned >= limit) return null
        rowsReturned++
        return baseScanner.nextOrNull()
    }

}