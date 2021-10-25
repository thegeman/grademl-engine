package science.atlarge.grademl.query

import science.atlarge.grademl.core.GradeMLJob
import science.atlarge.grademl.query.execution.ConcreteTable
import science.atlarge.grademl.query.execution.TablePrinterV2
import science.atlarge.grademl.query.execution.data.DefaultTables
import science.atlarge.grademl.query.language.*
import science.atlarge.grademl.query.plan.ExplainLogicalPlan
import science.atlarge.grademl.query.plan.ExplainPhysicalPlan
import science.atlarge.grademl.query.plan.QueryPlanner

class QueryEngine(
    gradeMLJob: GradeMLJob
) {

    private val builtinTables = DefaultTables.create(gradeMLJob)
    private val concreteTables = mutableMapOf<String, ConcreteTable>()
    private val tables = builtinTables.toMutableMap()

    fun executeStatement(statement: Statement) {
        when (statement) {
            is SelectStatement -> {
                val logicalPlan = QueryPlanner.createLogicalPlanFromSelect(statement, tables)
                val physicalQueryPlan = QueryPlanner.convertLogicalToPhysicalPlan(logicalPlan)
                val optimizedQueryPlan = QueryPlanner.optimizePhysicalPlan(physicalQueryPlan)
                TablePrinterV2.print(
                    optimizedQueryPlan.toQueryOperator().execute(),
                    limit = statement.limit?.limitFirst
                )
                println()
            }
            is CreateTableStatement -> {
                val tableName = statement.tableName.trim()
                require(tableName.isNotEmpty()) { "Table must be given a non-empty name" }
                require(tableName !in tables) { "Table with name \"$tableName\" already exists" }

                val logicalPlan = QueryPlanner.createLogicalPlanFromSelect(statement.tableDefinition, tables)
                val physicalQueryPlan = QueryPlanner.convertLogicalToPhysicalPlan(logicalPlan)
                val optimizedQueryPlan = QueryPlanner.optimizePhysicalPlan(physicalQueryPlan)

                TODO("Convert query plan to Table object")

//                tables[tableName] = ...

//                println("Table \"$tableName\" created.")
//                println()
            }
            is DeleteTableStatement -> {
                val tableName = statement.tableName.trim()
                require(tableName.isNotEmpty()) { "Cannot delete table with an empty name" }
                require(tableName in tables) { "Table with name \"$tableName\" does not exist" }
                require(tableName !in builtinTables) { "Cannot delete built-in table \"$tableName\"" }

                concreteTables.remove(tableName)
                tables.remove(tableName)

                println("Table \"$tableName\" deleted.")
                println()
            }
            is CacheTableStatement -> {
                val tableName = statement.tableName.trim()
                require(tableName.isNotEmpty()) { "Cannot delete table with an empty name" }
                require(tableName in tables) { "Table with name \"$tableName\" does not exist" }

                if (tableName !in concreteTables) {
                    val concreteTable = ConcreteTable.from(tables[tableName]!!.timeSeriesIterator())
                    concreteTables[tableName] = concreteTable
                    tables[tableName] = concreteTable
                    println(
                        "Table \"$tableName\" with ${concreteTable.timeSeriesCount} time series and " +
                                "${concreteTable.rowCount} rows added to the cache."
                    )
                    println()
                } else {
                    println("Table \"$tableName\" was already in the cache.")
                    println()
                }
            }
            is DropTableFromCacheStatement -> {
                val tableName = statement.tableName.trim()
                require(tableName.isNotEmpty()) { "Cannot delete table with an empty name" }

                concreteTables.remove(tableName) ?: throw IllegalArgumentException(
                    "Table with name \"$tableName\" does not exist or is not cached"
                )
                tables.remove(tableName)

                println("Table \"$tableName\" dropped from the cache.")
                println()
            }
            is ExplainStatement -> {
                println()
                val logicalPlan = QueryPlanner.createLogicalPlanFromSelect(statement.selectStatement, tables)
                println("LOGICAL QUERY PLAN:")
                println(ExplainLogicalPlan.explain(logicalPlan))
                println()
                val physicalQueryPlan = QueryPlanner.convertLogicalToPhysicalPlan(logicalPlan)
                println("PHYSICAL QUERY PLAN:")
                println(ExplainPhysicalPlan.explain(physicalQueryPlan))
                println()
                val optimizedQueryPlan = QueryPlanner.optimizePhysicalPlan(physicalQueryPlan)
                println("OPTIMIZED PHYSICAL QUERY PLAN:")
                println(ExplainPhysicalPlan.explain(optimizedQueryPlan))
                println()
            }
        }
    }

}
