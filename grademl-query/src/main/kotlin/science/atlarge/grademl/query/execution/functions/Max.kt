package science.atlarge.grademl.query.execution.functions

import science.atlarge.grademl.query.execution.AggregatingFunctionImplementation
import science.atlarge.grademl.query.execution.Aggregator
import science.atlarge.grademl.query.language.FunctionDefinition
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.BuiltinFunctions
import science.atlarge.grademl.query.model.TypedValue

object Max : AggregatingFunctionImplementation {
    override val definition: FunctionDefinition = BuiltinFunctions.MAX
    override fun newAggregator(argumentTypes: List<Type>) = when (argumentTypes[0]) {
        Type.UNDEFINED -> throw IllegalArgumentException()
        Type.BOOLEAN -> object : Aggregator {
            private var anyAreTrue = false
            override fun reset() {
                anyAreTrue = false
            }

            override fun addRow(arguments: Array<TypedValue>) {
                if (arguments[0].booleanValue) anyAreTrue = true
            }

            override fun writeResultTo(outValue: TypedValue) {
                outValue.booleanValue = anyAreTrue
            }
        }
        Type.NUMERIC -> object : Aggregator {
            private var maxValue: Double? = null
            override fun reset() {
                maxValue = null
            }

            override fun addRow(arguments: Array<TypedValue>) {
                maxValue = if (maxValue == null) arguments[0].numericValue else
                    maxOf(maxValue!!, arguments[0].numericValue)
            }

            override fun writeResultTo(outValue: TypedValue) {
                outValue.numericValue = maxValue!!
            }
        }
        Type.STRING -> object : Aggregator {
            private var maxValue: String? = null
            override fun reset() {
                maxValue = null
            }

            override fun addRow(arguments: Array<TypedValue>) {
                maxValue = if (maxValue == null) arguments[0].stringValue else
                    maxOf(maxValue!!, arguments[0].stringValue)
            }

            override fun writeResultTo(outValue: TypedValue) {
                outValue.stringValue = maxValue!!
            }
        }
    }
}