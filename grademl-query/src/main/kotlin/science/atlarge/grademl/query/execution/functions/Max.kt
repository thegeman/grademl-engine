package science.atlarge.grademl.query.execution.functions

import science.atlarge.grademl.query.execution.*
import science.atlarge.grademl.query.language.FunctionDefinition
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.BuiltinFunctions
import science.atlarge.grademl.query.model.TypedValue
import science.atlarge.grademl.query.model.v2.Row

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

    override fun newAggregator(
        argumentExpressions: List<PhysicalExpression>,
        argumentTypes: List<Type>
    ): AggregatorV2 {
        return when (argumentTypes[0]) {
            Type.UNDEFINED -> throw IllegalArgumentException()
            Type.BOOLEAN -> BooleanMax(argumentExpressions[0] as BooleanPhysicalExpression)
            Type.NUMERIC -> NumericMax(argumentExpressions[0] as NumericPhysicalExpression)
            Type.STRING -> StringMax(argumentExpressions[0] as StringPhysicalExpression)
        }
    }

    private class BooleanMax(private val expr: BooleanPhysicalExpression) : AggregatorV2 {
        private var maxValue = false
        override fun reset() {
            maxValue = false
        }

        override fun addRow(row: Row) {
            if (!maxValue) maxValue = expr.evaluateAsBoolean(row)
        }

        override fun getBooleanResult(): Boolean {
            return maxValue
        }
    }

    private class NumericMax(private val expr: NumericPhysicalExpression) : AggregatorV2 {
        private var maxValue = Double.NEGATIVE_INFINITY
        override fun reset() {
            maxValue = Double.NEGATIVE_INFINITY
        }

        override fun addRow(row: Row) {
            maxValue = maxOf(maxValue, expr.evaluateAsNumeric(row))
        }

        override fun getNumericResult(): Double {
            return maxValue
        }
    }

    private class StringMax(private val expr: StringPhysicalExpression) : AggregatorV2 {
        private var maxValue: String? = null
        override fun reset() {
            maxValue = null
        }

        override fun addRow(row: Row) {
            val valueOfRow = expr.evaluateAsString(row)
            maxValue = if (maxValue == null) valueOfRow else maxOf(maxValue!!, valueOfRow)
        }

        override fun getStringResult(): String {
            return maxValue!!
        }
    }
}