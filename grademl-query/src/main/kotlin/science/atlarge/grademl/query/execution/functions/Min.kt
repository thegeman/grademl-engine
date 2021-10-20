package science.atlarge.grademl.query.execution.functions

import science.atlarge.grademl.query.execution.*
import science.atlarge.grademl.query.language.FunctionDefinition
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.BuiltinFunctions
import science.atlarge.grademl.query.model.TypedValue
import science.atlarge.grademl.query.model.v2.Row

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

    override fun newAggregator(
        argumentExpressions: List<PhysicalExpression>,
        argumentTypes: List<Type>
    ): AggregatorV2 {
        return when (argumentTypes[0]) {
            Type.UNDEFINED -> throw IllegalArgumentException()
            Type.BOOLEAN -> BooleanMin(argumentExpressions[0] as BooleanPhysicalExpression)
            Type.NUMERIC -> NumericMin(argumentExpressions[0] as NumericPhysicalExpression)
            Type.STRING -> StringMin(argumentExpressions[0] as StringPhysicalExpression)
        }
    }

    private class BooleanMin(private val expr: BooleanPhysicalExpression) : AggregatorV2 {
        private var minValue = true
        override fun reset() {
            minValue = true
        }

        override fun addRow(row: Row) {
            if (minValue) minValue = expr.evaluateAsBoolean(row)
        }

        override fun getBooleanResult(): Boolean {
            return minValue
        }
    }

    private class NumericMin(private val expr: NumericPhysicalExpression) : AggregatorV2 {
        private var minValue = Double.POSITIVE_INFINITY
        override fun reset() {
            minValue = Double.POSITIVE_INFINITY
        }

        override fun addRow(row: Row) {
            minValue = minOf(minValue, expr.evaluateAsNumeric(row))
        }

        override fun getNumericResult(): Double {
            return minValue
        }
    }

    private class StringMin(private val expr: StringPhysicalExpression) : AggregatorV2 {
        private var minValue: String? = null
        override fun reset() {
            minValue = null
        }

        override fun addRow(row: Row) {
            val valueOfRow = expr.evaluateAsString(row)
            minValue = if (minValue == null) valueOfRow else minOf(minValue!!, valueOfRow)
        }

        override fun getStringResult(): String {
            return minValue!!
        }
    }
}