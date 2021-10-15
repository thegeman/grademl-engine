package science.atlarge.grademl.query.execution.functions

import science.atlarge.grademl.query.execution.MappingFunctionImplementation
import science.atlarge.grademl.query.model.BuiltinFunctions
import science.atlarge.grademl.query.model.TypedValue

object AsNumeric : MappingFunctionImplementation {
    override val definition = BuiltinFunctions.AS_NUMERIC
    override fun computeValue(arguments: List<TypedValue>, argumentCount: Int, outValue: TypedValue) {
        outValue.numericValue = if (arguments[0].booleanValue) 1.0 else 0.0
    }
}