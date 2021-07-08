package science.atlarge.grademl.query.execution.functions

import science.atlarge.grademl.query.execution.MappingFunctionImplementation
import science.atlarge.grademl.query.model.BuiltinFunctions
import science.atlarge.grademl.query.model.TypedValue

object MinOf : MappingFunctionImplementation {
    override val definition = BuiltinFunctions.MIN_OF
    override fun computeValue(arguments: List<TypedValue>, argumentCount: Int, outValue: TypedValue) {
        arguments[0].copyTo(outValue)
        for (i in 1 until argumentCount) {
            if (arguments[i] < outValue) arguments[i].copyTo(outValue)
        }
    }
}