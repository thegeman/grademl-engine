package science.atlarge.grademl.query.language

enum class Type {
    BOOLEAN,
    NUMERIC,
    STRING
}

interface Typed {
    val type: Type
}