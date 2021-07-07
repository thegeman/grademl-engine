package science.atlarge.grademl.query.execution.functions

import science.atlarge.grademl.query.PathUtils
import science.atlarge.grademl.query.execution.MappingFunctionImplementation
import science.atlarge.grademl.query.model.BuiltinFunctions
import science.atlarge.grademl.query.model.TypedValue

object IsDescendantOf : MappingFunctionImplementation {
    override val definition = BuiltinFunctions.IS_DESCENDANT_OF
    override fun computeValue(arguments: List<TypedValue>, outValue: TypedValue) {
        val leftPath = arguments[0].stringValue
        val rightPath = arguments[1].stringValue
        outValue.booleanValue = leftPath.isDescendantOf(rightPath)
    }

    fun String.isDescendantOf(other: String): Boolean {
        // Run simple checks first
        if (isEmpty() || other.isEmpty()) return false
        if (length < other.length + 1) return false
        if (!startsWith(other)) return false

        // Check if the parent path ends in a separator
        if (other.last() in PathUtils.SEPARATORS) return true

        // Check if the next character in this path is a separator
        if (get(other.length) !in PathUtils.SEPARATORS) return false

        // Make sure there are additional characters
        return length > other.length + 1
    }
}