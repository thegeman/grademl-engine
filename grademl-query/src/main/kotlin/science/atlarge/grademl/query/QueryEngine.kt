package science.atlarge.grademl.query

import science.atlarge.grademl.core.GradeMLJob
import science.atlarge.grademl.query.analysis.ASTAnalysis
import science.atlarge.grademl.query.execution.*
import science.atlarge.grademl.query.language.*
import science.atlarge.grademl.query.model.DefaultTables
import science.atlarge.grademl.query.model.Table

class QueryEngine(
    private val gradeMLJob: GradeMLJob
) {

    private val tables = DefaultTables.create(gradeMLJob)

    fun runStatement(statement: Statement) {
        when (statement) {
            is SelectStatement -> runSelect(statement)
        }
    }

    fun runSelect(selectStatement: SelectStatement) {
        // Execute the FROM clause
        val inputTable = applyFrom(selectStatement.from, tables)

        // Execute the WHERE clause
        val filteredTable =
            if (selectStatement.where != null) applyWhere(selectStatement.where, inputTable)
            else inputTable

        // Execute the GROUP BY clause
        val groupedTable =
            if (selectStatement.groupBy != null) applyGroupBy(selectStatement.groupBy, filteredTable)
            else filteredTable

        // Execute the SELECT clause
        val projectedTable = applySelect(selectStatement.select, groupedTable)

        // Display the result (with the LIMIT clause)
        TablePrinter.print(
            projectedTable,
            showFirst = selectStatement.limit?.limitFirst ?: 10,
            showLast = selectStatement.limit?.limitLast ?: 10
        )
        println()
    }

    private fun applyFrom(fromClause: FromClause, tables: Map<String, Table>): Table {
        // Derive a virtual table from (an) existing table(s)
        val baseTable = tables[fromClause.tableName] ?: throw IllegalArgumentException(
            "Table ${fromClause.tableName} does not exist"
        )
        return AliasedTable(baseTable, fromClause.alias ?: "")
    }

    private fun applyWhere(whereClause: WhereClause, table: Table): Table {
        // Analyze the filter condition
        val filterExpression = ASTAnalysis.analyzeExpression(whereClause.conditionExpression, table.columns)
        require(filterExpression.type == Type.BOOLEAN) {
            "WHERE clause requires a BOOLEAN expression as condition"
        }

        // Filter the input table
        return FilteredTable(table, filterExpression)
    }

    private fun applyGroupBy(groupByClause: GroupByClause, table: Table): Table {
        // Derive a clustered table
        val groupByColumns = groupByClause.columns.map { c -> ASTAnalysis.analyzeExpression(c, table.columns) }
        return ClusteredTable(table, groupByColumns.map { (it as ColumnLiteral).columnIndex })
    }

    private fun applySelect(selectClause: SelectClause, table: Table): Table {
        // Analyze the projection expressions
        val projections = selectClause.columnExpressions.map { ASTAnalysis.analyzeExpression(it, table.columns) }

        // Select appropriate column names
        val columnNames = mutableListOf<String>()
        for (i in projections.indices) {
            val columnName = when {
                selectClause.columnAliases[i] != null -> selectClause.columnAliases[i]!!
                projections[i] is ColumnLiteral -> (projections[i] as ColumnLiteral).columnName
                else -> "_$i"
            }
            if (columnName in columnNames)
                throw IllegalArgumentException("Duplicate column name in SELECT clause: $columnName")
            columnNames.add(columnName)
        }

        // Apply the projections
        return ProjectedTable(table, projections, columnNames)
    }

}
