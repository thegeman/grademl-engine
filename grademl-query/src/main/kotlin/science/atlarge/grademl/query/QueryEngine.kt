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
import science.atlarge.grademl.query.model.v2.Columns
import science.atlarge.grademl.query.model.v2.TableSchema
import science.atlarge.grademl.query.model.v2.TimeSeriesIterator
import science.atlarge.grademl.query.plan.ExplainLogicalPlan
import science.atlarge.grademl.query.plan.logical.LogicalQueryPlan
import science.atlarge.grademl.query.plan.logical.LogicalQueryPlanBuilder

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

    private fun createQueryPlanFromSelect(
        selectStatement: SelectStatement,
        builder: LogicalQueryPlanBuilder = LogicalQueryPlanBuilder()
    ): LogicalQueryPlan {
        // Parse the from clause
        require(selectStatement.from.tables.isNotEmpty()) { "Query must have at least one input" }
        require(selectStatement.from.aliases.size == 1 || selectStatement.from.aliases.none { it.isBlank() }) {
            "All inputs of a join must have an alias"
        }
        require(selectStatement.from.aliases.toSet().size == selectStatement.from.aliases.size) {
            "All inputs of a join must have a unique alias"
        }
        // Find the input data source(s)
        val inputPlans = selectStatement.from.tables.map { tableExpression ->
            when (tableExpression) {
                is TableExpression.Query -> createQueryPlanFromSelect(tableExpression.tableDefinition, builder)
                is TableExpression.NamedTable -> {
                    val tableName = tableExpression.tableName
                    val table = (cachedTables[tableName] ?: tables[tableName] ?: throw IllegalArgumentException(
                        "Table $tableName does not exist"
                    ))
                    val newTable = object : science.atlarge.grademl.query.model.v2.Table {
                        override val schema: TableSchema
                            get() = TableSchema(table.columns.map { column ->
                                science.atlarge.grademl.query.model.v2.Column(column.path, column.type, column.isStatic)
                            })

                        override fun timeSeriesIterator(): TimeSeriesIterator {
                            TODO("Not yet implemented")
                        }
                    }
                    builder.scan(newTable, tableName)
                }
            }
        }

        // Alias the input(s)
        val aliasesInputPlans = inputPlans.mapIndexed { index, input ->
            val alias = selectStatement.from.aliases[index]
            if (alias.isBlank()) input
            else {
                val reservedColumns = input.schema.columns.filter { it in Columns.RESERVED_COLUMNS }
                    .map { NamedExpression(ColumnLiteral(it.identifier), it.identifier) }
                val columnExpressions = input.schema.columns.map { column ->
                    NamedExpression(ColumnLiteral(column.identifier), "$alias.${column.identifier}")
                }
                builder.project(input, reservedColumns + columnExpressions)
            }
        }

        // Join the inputs (if applicable)
        val joinedInputPlan = aliasesInputPlans.reduce { leftInput, rightInput ->
            builder.temporalJoin(leftInput, rightInput)
        }

        // Apply the where clause
        val filteredInputPlan = if (selectStatement.where == null) {
            joinedInputPlan
        } else {
            val filterExpression = ASTAnalysis.analyzeExpressionV2(
                selectStatement.where.conditionExpression, joinedInputPlan.schema.columns
            )
            builder.filter(joinedInputPlan, filterExpression)
        }

        // Parse the group by clause
        val groupByColumns = if (selectStatement.groupBy == null) emptyList() else selectStatement.groupBy.columns
        // Parse the select clause
        val resolvedSelectTerms = selectStatement.select.terms
            .flatMap {
                when (it) {
                    is SelectTerm.Anonymous -> listOf(it.expression to null)
                    is SelectTerm.Named -> listOf(it.namedExpression.expr to it.namedExpression.name)
                    SelectTerm.Wildcard -> filteredInputPlan.schema.columns.map { column ->
                        ColumnLiteral(column.identifier) to column.identifier
                    }
                }
            }
            .map {
                ASTAnalysis.analyzeExpressionV2(it.first, filteredInputPlan.schema.columns) to it.second
            }
            .mapIndexed { index, (expression, name) ->
                // Assign names to anonymous select terms
                NamedExpression(
                    expression, when {
                        name != null -> name
                        expression is ColumnLiteral -> expression.columnPath
                        else -> "_$index"
                    }
                )
            }

        // Check for duplicate names
        require(resolvedSelectTerms.map { it.name }.toSet().size == resolvedSelectTerms.size) {
            "Found duplicate column names in SELECT: [${
                resolvedSelectTerms.groupBy { it.name }.filter { it.value.size > 1 }.keys.joinToString()
            }]"
        }

        // Check if the query has a group-by clause or any aggregating functions
        val isAggregatingSelect = groupByColumns.isNotEmpty() ||
                resolvedSelectTerms.any { ASTUtils.findFunctionCalls(it.expr).isNotEmpty() }

        // Create an AggregatePlan for an aggregating queries, or a ProjectPlan otherwise
        val projectedOutputPlan = if (isAggregatingSelect) {
            builder.aggregate(filteredInputPlan, groupByColumns, resolvedSelectTerms)
        } else {
            builder.project(filteredInputPlan, resolvedSelectTerms)
        }

        // Sort if needed, then return the compiled query plan
        return if (selectStatement.orderBy != null) {
            val sortColumns = selectStatement.orderBy.columns.zip(selectStatement.orderBy.ascending)
                .map { (col, asc) -> SortColumn(col, asc) }
            builder.sort(projectedOutputPlan, sortColumns)
        } else {
            projectedOutputPlan
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
                is TableExpression.Query -> createTableFromSelect(tableId.tableDefinition)
                is TableExpression.NamedTable -> {
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
                is SelectTerm.Anonymous -> listOf(it.expression to null)
                is SelectTerm.Named -> listOf(it.namedExpression.expr to it.namedExpression.name)
                SelectTerm.Wildcard -> joinedInput.columns.map { column ->
                    ColumnLiteral(column.path) to column.path
                }
            }
        }
        val projectionExpressions = resolvedSelectTerms.map {
            ASTAnalysis.analyzeExpression(it.first, joinedInput.columns)
        }
        val columnNames = mutableListOf<String>()
        // Select appropriate column names
        for (i in projectionExpressions.indices) {
            val columnName = when {
                resolvedSelectTerms[i].second != null -> resolvedSelectTerms[i].second!!
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
