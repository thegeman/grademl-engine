package science.atlarge.grademl.query.execution.functions

import science.atlarge.grademl.query.PathUtils
import science.atlarge.grademl.query.execution.MappingFunctionImplementation
import science.atlarge.grademl.query.model.BuiltinFunctions
import science.atlarge.grademl.query.model.TypedValue

object ParentOf : MappingFunctionImplementation {
    override val definition = BuiltinFunctions.PARENT_OF
    override fun computeValue(arguments: List<TypedValue>, argumentCount: Int, outValue: TypedValue) {
        val path = arguments[0].stringValue
        val lastSeparator = path.lastIndexOfAny(PathUtils.SEPARATORS)
        if (lastSeparator <= 0) {
            outValue.stringValue = ""
        } else {
            outValue.stringValue = path.substring(0, lastSeparator)
        }
    }
}