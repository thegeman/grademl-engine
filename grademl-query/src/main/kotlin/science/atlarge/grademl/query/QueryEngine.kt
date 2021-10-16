package science.atlarge.grademl.query

import science.atlarge.grademl.core.GradeMLJob
import science.atlarge.grademl.query.analysis.ASTAnalysis
import science.atlarge.grademl.query.analysis.ASTUtils
import science.atlarge.grademl.query.execution.*
import science.atlarge.grademl.query.execution.data.DefaultTables
import science.atlarge.grademl.query.language.*
import science.atlarge.grademl.query.model.Column
import science.atlarge.grademl.query.model.ColumnFunction
import science.atlarge.grademl.query.model.Table

class QueryEngine(
    gradeMLJob: GradeMLJob
) {

    private val builtinTables = DefaultTables.create(gradeMLJob)
    private val tables = builtinTables.toMutableMap()
    private val cachedTables = mutableMapOf<String, ConcreteTable>()

    fun executeStatement(statement: Statement) {
        when (statement) {
            is SelectStatement -> {
                TablePrinter.print(
                    createTableFromSelect(statement),
                    limit = statement.limit?.limitFirst
                )
                println()
            }
            is CreateTableStatement -> {
                val tableName = statement.tableName.trim()
                require(tableName.isNotEmpty()) { "Table must be given a non-empty name" }
                require(tableName !in tables) { "Table with name \"$tableName\" already exists" }

                val tableDefinition = createTableFromSelect(statement.tableDefinition)

                tables[tableName] = tableDefinition

                println("Table \"$tableName\" created.")
                println()
            }
            is DeleteTableStatement -> {
                val tableName = statement.tableName.trim()
                require(tableName.isNotEmpty()) { "Cannot delete table with an empty name" }
                require(tableName in tables) { "Table with name \"$tableName\" does not exist" }
                require(tableName !in builtinTables) { "Cannot delete built-in table \"$tableName\"" }

                cachedTables.remove(tableName)
                tables.remove(tableName)

                println("Table \"$tableName\" deleted.")
                println()
            }
            is CacheTableStatement -> {
                val tableName = statement.tableName.trim()
                require(tableName.isNotEmpty()) { "Cannot delete table with an empty name" }
                require(tableName in tables) { "Table with name \"$tableName\" does not exist" }

                if (tableName !in cachedTables) {
                    cachedTables[tableName] = ConcreteTable.from(tables[tableName]!!)
                    println("Table \"$tableName\" with ${cachedTables[tableName]!!.rowCount} rows added to the cache.")
                    println()
                } else {
                    println("Table \"$tableName\" was already in the cache.")
                    println()
                }
            }
            is DropTableFromCacheStatement -> {
                val tableName = statement.tableName.trim()
                require(tableName.isNotEmpty()) { "Cannot delete table with an empty name" }
                require(tableName in cachedTables) {
                    "Table with name \"$tableName\" does not exist or is not cached"
                }

                cachedTables.remove(tableName)

                println("Table \"$tableName\" dropped from the cache.")
                println()
            }
        }
    }

    private fun createTableFromSelect(selectStatement: SelectStatement): Table {
        // Parse the from clause
        require(selectStatement.from.tables.isNotEmpty()) { "Query must have at least one input" }
        require(selectStatement.from.aliases.size == 1 || selectStatement.from.aliases.none { it.isBlank() }) {
            "All inputs of a join must have an alias"
        }
        require(selectStatement.from.aliases.toSet().size == selectStatement.from.aliases.size) {
            "All inputs of a join must have a unique alias"
        }
        // Find the input data source(s)
        val inputTables = selectStatement.from.tables.map { tableId ->
            when (tableId) {
                is TableIdentifier.AnonymousTable -> createTableFromSelect(tableId.tableDefinition)
                is TableIdentifier.NamedTable -> {
                    val tableName = tableId.tableName
                    cachedTables[tableName] ?: tables[tableName] ?: throw IllegalArgumentException(
                        "Table $tableName does not exist"
                    )
                }
            }
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
        val resolvedSelectTerms = selectStatement.select.terms.flatMap {
            when (it) {
                is SelectTerm.FromExpression -> listOf(it)
                SelectTerm.Wildcard -> joinedInput.columns.map { column ->
                    SelectTerm.FromExpression(ColumnLiteral(column.path), null)
                }
            }
        }
        val projectionExpressions = resolvedSelectTerms.map {
            ASTAnalysis.analyzeExpression(it.expression, joinedInput.columns)
        }
        val columnNames = mutableListOf<String>()
        // Select appropriate column names
        for (i in projectionExpressions.indices) {
            val columnName = when {
                resolvedSelectTerms[i].alias != null -> resolvedSelectTerms[i].alias!!
                projectionExpressions[i] is ColumnLiteral -> (projectionExpressions[i] as ColumnLiteral).columnName
                else -> "_$i"
            }
            if (columnName in columnNames)
                throw IllegalArgumentException("Duplicate column name in SELECT clause: $columnName")
            columnNames.add(columnName)
        }
        val projections = projectionExpressions.indices.map { i ->
            Column(
                columnNames[i],
                columnNames[i],
                i,
                projectionExpressions[i].type,
                determineColumnFunction(columnNames[i], projectionExpressions[i], joinedInput.columns) in
                        setOf(ColumnFunction.METADATA, ColumnFunction.KEY)
            ) to projectionExpressions[i]
        }

        // Parse the order by clause
        val sortColumns = if (selectStatement.orderBy == null) emptyList() else {
            selectStatement.orderBy.columns.zip(selectStatement.orderBy.ascending).map { (col, asc) ->
                SortColumn(col, asc)
            }
        }

        return DerivedTable.from(joinedInput, filterExpression, groupByColumns, projections, sortColumns)
    }

    private fun determineColumnFunction(
        name: String,
        expression: Expression,
        inputColumns: List<Column>
    ): ColumnFunction {
        // Check for reserved column names
        if (name == "_start_time") return ColumnFunction.TIME_START
        if (name == "_end_time") return ColumnFunction.TIME_END

        // Otherwise, the column type is TAG iff all inputs are TAGs
//        val usedColumns = ASTUtils.findColumnLiterals(expression)
//        val allTags = usedColumns.all { inputColumns[it.columnIndex].function == ColumnFunction.TAG }
//        return if (allTags) ColumnFunction.TAG else ColumnFunction.VALUE

        // TODO: Determine column type
        val usedColumns = ASTUtils.findColumnLiterals(expression)
        val anyValueTag = usedColumns.any { !inputColumns[it.columnIndex].isStatic }
        return if (anyValueTag) ColumnFunction.VALUE else ColumnFunction.METADATA
    }

}
