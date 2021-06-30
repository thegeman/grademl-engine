package science.atlarge.grademl.query.model

import science.atlarge.grademl.query.language.Expression

interface Table {

    val columns: List<Column>

    // Linear table scan
    fun scan(): RowScanner

    // Grouped table scan
    val isGrouped: Boolean
    fun scanGroups(): RowGroupScanner {
        throw UnsupportedOperationException()
    }

    // Push-down filter
    val supportsPushDownFilters: Boolean
    fun tryPushDownFilter(filterExpression: Expression): Table? {
        throw UnsupportedOperationException()
    }

    // Push-down projection
    val supportsPushDownProjections: Boolean
    fun tryPushDownProjection(projectionExpressions: List<Expression>): Table? {
        throw UnsupportedOperationException()
    }

    // Push-down sort
    val supportsPushDownSort: Boolean
    fun tryPushDownSort(sortColumns: List<Int>): Table? {
        throw UnsupportedOperationException()
    }

    // Push-down group-by
    val supportsPushDownGroupBy: Boolean
    fun tryPushDownGroupBy(groupColumns: List<Int>): Table? {
        throw UnsupportedOperationException()
    }

}