package science.atlarge.grademl.query.execution.functions

import science.atlarge.grademl.query.execution.AggregatingFunctionImplementation
import science.atlarge.grademl.query.execution.Aggregator
import science.atlarge.grademl.query.language.FunctionDefinition
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.BuiltinFunctions
import science.atlarge.grademl.query.model.TypedValue

object Min : AggregatingFunctionImplementation {
    override val definition: FunctionDefinition = BuiltinFunctions.MIN
    override fun newAggregator(argumentTypes: List<Type>) = when (argumentTypes[0]) {
        Type.UNDEFINED -> throw IllegalArgumentException()
        Type.BOOLEAN -> object : Aggregator {
            private var allAreTrue = true
            override fun reset() {
                allAreTrue = true
            }

            override fun addRow(arguments: Array<TypedValue>) {
                if (!arguments[0].booleanValue) allAreTrue = false
            }

            override fun writeResultTo(outValue: TypedValue) {
                outValue.booleanValue = allAreTrue
            }
        }
        Type.NUMERIC -> object : Aggregator {
            private var minValue: Double? = null
            override fun reset() {
                minValue = null
            }

            override fun addRow(arguments: Array<TypedValue>) {
                minValue = if (minValue == null) arguments[0].numericValue else
                    minOf(minValue!!, arguments[0].numericValue)
            }

            override fun writeResultTo(outValue: TypedValue) {
                outValue.numericValue = minValue!!
            }
        }
        Type.STRING -> object : Aggregator {
            private var minValue: String? = null
            override fun reset() {
                minValue = null
            }

            override fun addRow(arguments: Array<TypedValue>) {
                minValue = if (minValue == null) arguments[0].stringValue else
                    minOf(minValue!!, arguments[0].stringValue)
            }

            override fun writeResultTo(outValue: TypedValue) {
                outValue.stringValue = minValue!!
            }
        }
    }
}