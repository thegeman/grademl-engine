package science.atlarge.grademl.query

import com.github.h0tk3y.betterParse.grammar.tryParseToEnd
import com.github.h0tk3y.betterParse.parser.ErrorResult
import com.github.h0tk3y.betterParse.parser.Parsed
import science.atlarge.grademl.query.parsing.QueryGrammar

val Any?.ensureExhaustive: Unit
    get() {}

fun <T> Iterator<T>.nextOrNull(): T? {
    if (!hasNext()) return null
    return next()
}

fun QueryEngine.executeQuery(queryText: String): String {
    // Parse the query/queries
    val queries = when (val parseResult = QueryGrammar.tryParseToEnd(queryText)) {
        is Parsed -> parseResult.value
        is ErrorResult -> {
            return "Failed to parse query: $parseResult"
        }
    }

    // Run the queries
    val sb = StringBuilder()
    queries.forEach {
        try {
            executeStatement(it, sb)
        } catch (t: Throwable) {
            t.printStackTrace()
            println()
        }
    }
    return sb.toString()
}