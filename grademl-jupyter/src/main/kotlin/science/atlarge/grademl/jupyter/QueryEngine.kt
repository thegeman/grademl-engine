package science.atlarge.grademl.jupyter

import com.github.h0tk3y.betterParse.grammar.tryParseToEnd
import com.github.h0tk3y.betterParse.parser.ErrorResult
import com.github.h0tk3y.betterParse.parser.Parsed
import science.atlarge.grademl.core.GradeMLEngine
import science.atlarge.grademl.core.attribution.ResourceAttributionSettings
import science.atlarge.grademl.query.QueryEngine as InternalEngine
import science.atlarge.grademl.input.airflow.Airflow
import science.atlarge.grademl.input.resource_monitor.ResourceMonitor
import science.atlarge.grademl.input.spark.Spark
import science.atlarge.grademl.input.tensorflow.TensorFlow
import science.atlarge.grademl.query.execution.IntTypes
import science.atlarge.grademl.query.execution.IntTypes.toInt
import science.atlarge.grademl.query.execution.TablePrinter
import science.atlarge.grademl.query.execution.operators.QueryOperator
import science.atlarge.grademl.query.language.SelectStatement
import science.atlarge.grademl.query.parsing.QueryGrammar
import java.nio.file.Paths
import kotlin.system.measureNanoTime

class QueryEngine(
    inputPath: String,
    processedPath: String,
    queryOutputPath: String
) {

    private val queryEngine: InternalEngine

    init {
        // Register framework-specific GradeML plugins
        with(GradeMLEngine) {
            registerInputSource(ResourceMonitor)
            registerInputSource(Spark)
            registerInputSource(TensorFlow)
            registerInputSource(Airflow)
        }

        // Analyze collected performance data across multiple frameworks in an ML-workflow system using GradeML
        val gradeMLJob = GradeMLEngine.analyzeJob(
            listOf(Paths.get(inputPath)), Paths.get(processedPath),
            ResourceAttributionSettings(
                enableTimeSeriesCompression = true,
                enableRuleCaching = true,
                enableAttributionResultCaching = false
            )
        )

        // Start the GradeML query engine
        queryEngine = InternalEngine(gradeMLJob, Paths.get(queryOutputPath))
    }

    fun executeQuery(queryText: String, showAll: Boolean = false): QueryResult {
        // Parse the query/queries
        val queries = when (val parseResult = QueryGrammar.tryParseToEnd(queryText)) {
            is Parsed -> parseResult.value
            is ErrorResult -> {
                return QueryResult(null, "Failed to parse query: $parseResult")
            }
        }

        if (queries.size > 1) {
            return QueryResult(null, "executeQuery accepts only one query at a time")
        }

        // Run the query
        val result = try {
            when (val stmt = queries[0]) {
                is SelectStatement -> {
                    QueryResult(queryEngine.prepareSelect(stmt), "", showAll = showAll)
                }
                else -> {
                    val sb = StringBuilder()
                    queryEngine.executeStatement(stmt, sb)
                    QueryResult(null, sb.toString())
                }
            }
        } catch (t: Throwable) {
            QueryResult(null, t.stackTraceToString())
        }

        return result
    }

}

class QueryResult(
    private val outcome: QueryOperator?,
    private val messages: String,
    private val showAll: Boolean = false
) {
    fun toMap(): Map<String, List<*>> {
        if (outcome == null) return emptyMap()

        val cols = outcome.schema.columns.size
        val colTypes = outcome.schema.columns.map { it.type.toInt() }

        val rowColumn = ArrayList<Int>()
        val tsColumn = ArrayList<Int>()
        val columns = Array(cols) { ArrayList<Any>() }
        val iterator = outcome.execute()
        var rowId = 0
        var tsId = 0
        while (iterator.loadNext()) {
            val ts = iterator.currentTimeSeries
            val rows = ts.rowIterator()
            tsId++

            while (rows.loadNext()) {
                val row = rows.currentRow
                rowId++
                rowColumn.add(rowId)
                tsColumn.add(tsId)
                for (i in 0 until cols) {
                    when (colTypes[i]) {
                        IntTypes.TYPE_BOOLEAN -> columns[i].add(row.getBoolean(i))
                        IntTypes.TYPE_NUMERIC -> columns[i].add(row.getNumeric(i))
                        IntTypes.TYPE_STRING -> columns[i].add(row.getString(i))
                        else -> throw IllegalArgumentException("Unsupported column type")
                    }
                }
            }
        }

        val map = HashMap<String, List<*>>()
        map["ROW"] = rowColumn
        map["TS"] = tsColumn
        for (i in 0 until cols) {
            map[outcome.schema.column(i).identifier] = columns[i]
        }
        return map
    }

    override fun toString(): String {
        if (outcome == null) return messages

        val sb = StringBuilder()
        val nanoseconds = measureNanoTime {
            TablePrinter.print(
                outcome.execute(),
                output = sb,
                maxLines = if (showAll) null else 50
            )
        }
        sb.appendLine("Query completed in ${(nanoseconds + 500000) / 1000000} ms")
        return sb.toString()
    }
}