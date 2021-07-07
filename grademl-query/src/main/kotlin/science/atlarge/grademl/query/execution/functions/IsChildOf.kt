package science.atlarge.grademl.query.execution.functions

import science.atlarge.grademl.query.PathUtils
import science.atlarge.grademl.query.execution.MappingFunctionImplementation
import science.atlarge.grademl.query.model.BuiltinFunctions
import science.atlarge.grademl.query.model.TypedValue

object IsChildOf : MappingFunctionImplementation {
    override val definition = BuiltinFunctions.IS_CHILD_OF
    override fun computeValue(arguments: List<TypedValue>, outValue: TypedValue) {
        val leftPath = arguments[0].stringValue
        val rightPath = arguments[1].stringValue
        outValue.booleanValue = leftPath.isChildOf(rightPath)
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