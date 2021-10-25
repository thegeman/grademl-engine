package science.atlarge.grademl.query.execution.functions

import science.atlarge.grademl.query.PathUtils
import science.atlarge.grademl.query.execution.BooleanPhysicalExpression
import science.atlarge.grademl.query.execution.MappingFunctionImplementation
import science.atlarge.grademl.query.execution.PhysicalExpression
import science.atlarge.grademl.query.execution.StringPhysicalExpression
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.BuiltinFunctions
import science.atlarge.grademl.query.model.v2.Row

object IsChildOf : MappingFunctionImplementation {

    override val definition = BuiltinFunctions.IS_CHILD_OF

    override fun toPhysicalExpression(
        arguments: List<PhysicalExpression>,
        argumentTypes: List<Type>
    ): BooleanPhysicalExpression {
        require(arguments.size == 2) { "IS_CHILD_OF takes two arguments" }
        require(argumentTypes[0] == Type.STRING && argumentTypes[1] == Type.STRING) {
            "IS_CHILD_OF takes two STRING arguments"
        }

        return object : BooleanPhysicalExpression {
            private val leftExpr = arguments[0] as StringPhysicalExpression
            private val rightExpr = arguments[1] as StringPhysicalExpression
            override fun evaluateAsBoolean(row: Row) =
                leftExpr.evaluateAsString(row).isChildOf(rightExpr.evaluateAsString(row))
        }
    }

    fun String.isChildOf(other: String): Boolean {
        // Run simple checks first
        if (isEmpty() || other.isEmpty()) return false
        if (length < other.length + 1) return false
        if (!startsWith(other)) return false

        // Check if the parent path ends in a separator
        if (other.last() in PathUtils.SEPARATORS) {
            // Make sure there is no additional separator in the child path
            val lastSeparator = lastIndexOfAny(PathUtils.SEPARATORS)
            return lastSeparator == other.lastIndex
        }

        // Check if the next character in this path is a separator
        if (get(other.length) !in PathUtils.SEPARATORS) return false

        // Make sure there is no additional separator in the child path, but there are other additional characters
        val lastSeparator = lastIndexOfAny(PathUtils.SEPARATORS)
        return lastSeparator == other.lastIndex + 1 && length > other.length + 1
    }

}