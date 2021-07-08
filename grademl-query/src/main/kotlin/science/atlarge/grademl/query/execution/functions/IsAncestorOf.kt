package science.atlarge.grademl.query.execution.functions

import science.atlarge.grademl.query.execution.MappingFunctionImplementation
import science.atlarge.grademl.query.execution.functions.IsDescendantOf.isDescendantOf
import science.atlarge.grademl.query.model.BuiltinFunctions
import science.atlarge.grademl.query.model.TypedValue

object IsAncestorOf : MappingFunctionImplementation {
    override val definition = BuiltinFunctions.IS_ANCESTOR_OF
    override fun computeValue(arguments: List<TypedValue>, argumentCount: Int, outValue: TypedValue) {
        val leftPath = arguments[0].stringValue
        val rightPath = arguments[1].stringValue
        outValue.booleanValue = rightPath.isDescendantOf(leftPath)
    }
}