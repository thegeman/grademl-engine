package science.atlarge.grademl.query.execution.functions

import science.atlarge.grademl.query.execution.MappingFunctionImplementation
import science.atlarge.grademl.query.execution.functions.IsChildOf.isChildOf
import science.atlarge.grademl.query.model.BuiltinFunctions
import science.atlarge.grademl.query.model.TypedValue

object IsParentOf : MappingFunctionImplementation {
    override val definition = BuiltinFunctions.IS_PARENT_OF
    override fun computeValue(arguments: List<TypedValue>, outValue: TypedValue) {
        val leftPath = arguments[0].stringValue
        val rightPath = arguments[1].stringValue
        outValue.booleanValue = rightPath.isChildOf(leftPath)
    }
}