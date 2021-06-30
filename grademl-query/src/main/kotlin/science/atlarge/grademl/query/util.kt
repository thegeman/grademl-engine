package science.atlarge.grademl.query

val Any?.ensureExhaustive: Unit
    get() {}

fun <T> Iterator<T>.nextOrNull(): T? {
    if (!hasNext()) return null
    return next()
}