package science.atlarge.grademl.query

import science.atlarge.grademl.core.GradeMLJob
import science.atlarge.grademl.query.analysis.ASTAnalysis
import science.atlarge.grademl.query.execution.AliasedTable
import science.atlarge.grademl.query.execution.TablePrinter
import science.atlarge.grademl.query.execution.DerivedTable
import science.atlarge.grademl.query.execution.data.DefaultTables
import science.atlarge.grademl.query.language.ColumnLiteral
import science.atlarge.grademl.query.language.SelectStatement
import science.atlarge.grademl.query.language.Statement
import science.atlarge.grademl.query.model.Column
import science.atlarge.grademl.query.model.Table

class QueryEngine(
    gradeMLJob: GradeMLJob
) {

    private val tables = DefaultTables.create(gradeMLJob)

    fun runStatement(statement: Statement) {
        when (statement) {
            is SelectStatement -> {
                TablePrinter.print(
                    runSelect(statement),
                    showFirst = statement.limit?.run { limitFirst ?: 0 } ?: 10,
                    showLast = statement.limit?.run { limitLast ?: 0 } ?: 10
                )
                println()
            }
        }
    }

    private fun runSelect(selectStatement: SelectStatement): Table {
        // Find the input data source
        val inputTable = tables[selectStatement.from.tableName] ?: throw IllegalArgumentException(
            "Table ${selectStatement.from.tableName} does not exist"
        )
        // Alias the input table if needed
        val aliasInputTable = if (selectStatement.from.alias == null) inputTable else
            AliasedTable(inputTable, selectStatement.from.alias)

        // Parse the where clause
        val filterExpression = if (selectStatement.where == null) null else
            ASTAnalysis.analyzeExpression(selectStatement.where.conditionExpression, aliasInputTable.columns)

        // Parse the group by clause
        val groupByColumns = if (selectStatement.groupBy == null) emptyList() else selectStatement.groupBy.columns

        // Parse the select clause
        val projectionExpressions = selectStatement.select.columnExpressions.map {
            ASTAnalysis.analyzeExpression(it, aliasInputTable.columns)
        }
        val columnNames = mutableListOf<String>()
        // Select appropriate column names
        for (i in projectionExpressions.indices) {
            val columnName = when {
                selectStatement.select.columnAliases[i] != null -> selectStatement.select.columnAliases[i]!!
                projectionExpressions[i] is ColumnLiteral -> (projectionExpressions[i] as ColumnLiteral).columnName
                else -> "_$i"
            }
            if (columnName in columnNames)
                throw IllegalArgumentException("Duplicate column name in SELECT clause: $columnName")
            columnNames.add(columnName)
        }
        val projections = projectionExpressions.indices.map { i ->
            Column(columnNames[i], columnNames[i], projectionExpressions[i].type) to projectionExpressions[i]
        }

        // Parse the order by clause
        val sortColumns = if (selectStatement.orderBy == null) emptyList() else selectStatement.orderBy.columns

        return DerivedTable.from(aliasInputTable, filterExpression, groupByColumns, projections, sortColumns)
    }

}
