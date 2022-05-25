package science.atlarge.grademl.query

import science.atlarge.grademl.core.GradeMLJob
import science.atlarge.grademl.query.execution.ConcreteTable
import science.atlarge.grademl.query.execution.TableExporter
import science.atlarge.grademl.query.execution.TablePrinter
import science.atlarge.grademl.query.execution.VirtualTable
import science.atlarge.grademl.query.execution.data.DefaultTables
import science.atlarge.grademl.query.execution.operators.QueryOperator
import science.atlarge.grademl.query.language.*
import science.atlarge.grademl.query.plan.ExplainLogicalPlan
import science.atlarge.grademl.query.plan.ExplainPhysicalPlan
import science.atlarge.grademl.query.plan.QueryPlanner
import science.atlarge.grademl.query.plan.StatisticsPhysicalPlan
import science.atlarge.grademl.query.plan.physical.PhysicalQueryPlan
import java.nio.file.Path
import kotlin.system.measureNanoTime

class QueryEngine(
    gradeMLJob: GradeMLJob,
    private val outputDirectory: Path
) {

    private val builtinTables = DefaultTables.create(gradeMLJob)
    private val concreteTables = mutableMapOf<String, ConcreteTable>()
    private val virtualTables = mutableMapOf<String, VirtualTable>()
    private val tables = builtinTables.toMutableMap()

    fun prepareSelect(statement: SelectStatement): QueryOperator {
        val optimizedQueryPlan = planSelect(statement)
        return optimizedQueryPlan.toQueryOperator()
    }

    fun executeStatement(statement: Statement, output: StringBuilder? = null) {
        fun localPrintln() = output?.appendLine() ?: println()
        fun localPrintln(s: String) = output?.appendLine(s) ?: println(s)

        when (statement) {
            is SelectStatement -> {
                val queryDurationNs = measureNanoTime {
                    val optimizedQueryPlan = planSelect(statement)
                    TablePrinter.print(
                        optimizedQueryPlan.toQueryOperator().execute(),
                        output = output
                    )
                }
                localPrintln("Query completed in ${(queryDurationNs + 500000) / 1000000} ms")
                localPrintln()
            }
            is CreateTableStatement -> {
                val tableName = statement.tableName.trim()
                require(tableName.isNotEmpty()) { "Table must be given a non-empty name" }
                require(tableName !in tables) { "Table with name \"$tableName\" already exists" }

                val logicalPlan = QueryPlanner.createLogicalPlanFromSelect(statement.tableDefinition, tables)
                val physicalQueryPlan = QueryPlanner.convertLogicalToPhysicalPlan(logicalPlan)
                val optimizedQueryPlan = QueryPlanner.optimizePhysicalPlan(physicalQueryPlan)

                val virtualTable = VirtualTable(logicalPlan, optimizedQueryPlan)
                virtualTables[tableName] = virtualTable
                tables[tableName] = virtualTable

                localPrintln("Table \"$tableName\" created.")
                localPrintln()
            }
            is DeleteTableStatement -> {
                val tableName = statement.tableName.trim()
                require(tableName.isNotEmpty()) { "Cannot delete table with an empty name" }
                require(tableName in tables) { "Table with name \"$tableName\" does not exist" }
                require(tableName !in builtinTables) { "Cannot delete built-in table \"$tableName\"" }

                concreteTables.remove(tableName)
                tables.remove(tableName)

                localPrintln("Table \"$tableName\" deleted.")
                localPrintln()
            }
            is CacheTableStatement -> {
                val tableName = statement.tableName.trim()
                require(tableName.isNotEmpty()) { "Cannot delete table with an empty name" }
                require(tableName in tables) { "Table with name \"$tableName\" does not exist" }

                if (tableName !in concreteTables) {
                    val concreteTable = ConcreteTable.from(tables[tableName]!!.timeSeriesIterator())
                    concreteTables[tableName] = concreteTable
                    tables[tableName] = concreteTable
                    localPrintln(
                        "Table \"$tableName\" with ${concreteTable.timeSeriesCount} time series and " +
                                "${concreteTable.rowCount} rows added to the cache."
                    )
                    localPrintln()
                } else {
                    localPrintln("Table \"$tableName\" was already in the cache.")
                    localPrintln()
                }
            }
            is DropTableFromCacheStatement -> {
                val tableName = statement.tableName.trim()
                require(tableName.isNotEmpty()) { "Cannot delete table with an empty name" }

                concreteTables.remove(tableName) ?: throw IllegalArgumentException(
                    "Table with name \"$tableName\" does not exist or is not cached"
                )
                tables.remove(tableName)

                // Reinstate virtual table from which the concrete table was created (if it exists)
                val virtualTable = virtualTables[tableName]
                if (virtualTable != null) {
                    tables[tableName] = virtualTable
                }

                localPrintln("Table \"$tableName\" dropped from the cache.")
                localPrintln()
            }
            is ExplainStatement -> {
                println()
                val logicalPlan = QueryPlanner.createLogicalPlanFromSelect(statement.selectStatement, tables)
                localPrintln("LOGICAL QUERY PLAN:")
                localPrintln(ExplainLogicalPlan.explain(logicalPlan))
                localPrintln()
                val physicalQueryPlan = QueryPlanner.convertLogicalToPhysicalPlan(logicalPlan)
                localPrintln("PHYSICAL QUERY PLAN:")
                localPrintln(ExplainPhysicalPlan.explain(physicalQueryPlan))
                localPrintln()
                val optimizedQueryPlan = QueryPlanner.optimizePhysicalPlan(physicalQueryPlan)
                localPrintln("OPTIMIZED PHYSICAL QUERY PLAN:")
                localPrintln(ExplainPhysicalPlan.explain(optimizedQueryPlan))
                localPrintln()
            }
            is StatisticsStatement -> {
                val optimizedQueryPlan = planSelect(statement.selectStatement)
                // Read as many rows as needed for the select statement
                var rowsRead = 0L
                val maxRows = statement.selectStatement.limit?.limit?.toLong() ?: Long.MAX_VALUE
                val tsIterator = optimizedQueryPlan.toQueryOperator().execute()
                while (rowsRead < maxRows && tsIterator.loadNext()) {
                    val rowIterator = tsIterator.currentTimeSeries.rowIterator()
                    while (rowsRead < maxRows && rowIterator.loadNext()) {
                        rowsRead++
                    }
                }
                // Print execution statistics
                localPrintln()
                localPrintln("EXECUTION STATISTICS PER PHYSICAL QUERY OPERATOR:")
                localPrintln(StatisticsPhysicalPlan.collectStatistics(optimizedQueryPlan))
                localPrintln()
            }
            is ExportStatement -> {
                // Create the output directory if needed
                val outputPath = outputDirectory.resolve(statement.filename)
                outputPath.parent.toFile().mkdirs()
                // Plan the query to be executed and exported
                val optimizedQueryPlan = planSelect(statement.selectStatement)
                // Export the query's output
                localPrintln("Exporting query output to ${outputPath.toAbsolutePath()}.")
                val rowsWritten = TableExporter.export(
                    outputPath,
                    optimizedQueryPlan.toQueryOperator().execute(),
                    statement.selectStatement.limit?.limit
                )
                localPrintln("Query produced $rowsWritten rows.")
                localPrintln()
            }
        }
    }

    private fun planSelect(selectStatement: SelectStatement): PhysicalQueryPlan =
        QueryPlanner.optimizePhysicalPlan(
            QueryPlanner.convertLogicalToPhysicalPlan(
                QueryPlanner.createLogicalPlanFromSelect(
                    selectStatement,
                    tables
                )
            )
        )

}
