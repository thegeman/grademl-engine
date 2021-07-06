package science.atlarge.grademl.query

import science.atlarge.grademl.core.GradeMLJob
import science.atlarge.grademl.query.analysis.ASTAnalysis
import science.atlarge.grademl.query.execution.AliasedTable
import science.atlarge.grademl.query.execution.TablePrinter
import science.atlarge.grademl.query.execution.DerivedTable
import science.atlarge.grademl.query.execution.TemporalJoinTable
import science.atlarge.grademl.query.execution.data.DefaultTables
import science.atlarge.grademl.query.language.ColumnLiteral
import science.atlarge.grademl.query.language.SelectStatement
import science.atlarge.grademl.query.language.Statement
import science.atlarge.grademl.query.model.Column
import science.atlarge.grademl.query.model.ColumnFunction
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
        // Parse the from clause
        require(selectStatement.from.tableNames.isNotEmpty()) { "Query must have at least one input" }
        require(selectStatement.from.aliases.size == 1 || selectStatement.from.aliases.none { it.isBlank() }) {
            "All inputs of a join must have an alias"
        }
        require(selectStatement.from.aliases.toSet().size == selectStatement.from.aliases.size) {
            "All inputs of a join must have a unique alias"
        }
        // Find the input data source(s)
        val inputTables = selectStatement.from.tableNames.map { tableName ->
            tables[tableName] ?: throw IllegalArgumentException(
                "Table ${selectStatement.from.tableNames[0]} does not exist"
            )
        }
        // Alias the input table(s) if needed
        val aliasInputTables = inputTables.mapIndexed { index, table ->
            val alias = selectStatement.from.aliases[index]
            if (alias.isBlank()) table
            else AliasedTable(table, alias.trim())
        }

        // Join the input tables if applicable
        val joinedInput = if (aliasInputTables.size > 1) {
            TemporalJoinTable.from(aliasInputTables)
        } else {
            aliasInputTables[0]
        }

        // Parse the where clause
        val filterExpression = if (selectStatement.where == null) null else
            ASTAnalysis.analyzeExpression(selectStatement.where.conditionExpression, joinedInput.columns)

        // Parse the group by clause
        val groupByColumns = if (selectStatement.groupBy == null) emptyList() else selectStatement.groupBy.columns

        // Parse the select clause
        val projectionExpressions = selectStatement.select.columnExpressions.map {
            ASTAnalysis.analyzeExpression(it, joinedInput.columns)
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
            val columnFunction = when (columnNames[i]) {
                "start_time" -> ColumnFunction.TIME_START
                "end_time" -> ColumnFunction.TIME_END
                "duration" -> ColumnFunction.TIME_DURATION
                else -> ColumnFunction.OTHER
            }
            Column(columnNames[i], columnNames[i], projectionExpressions[i].type, columnFunction) to projectionExpressions[i]
        }

        // Parse the order by clause
        val sortColumns = if (selectStatement.orderBy == null) emptyList() else selectStatement.orderBy.columns

        return DerivedTable.from(joinedInput, filterExpression, groupByColumns, projections, sortColumns)
    }

}
