package science.atlarge.grademl.query.execution.functions

import science.atlarge.grademl.query.execution.*
import science.atlarge.grademl.query.language.FunctionDefinition
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.BuiltinFunctions
import science.atlarge.grademl.query.model.TypedValue
import science.atlarge.grademl.query.model.v2.Row

object WeightedAverage : AggregatingFunctionImplementation {
    override val definition: FunctionDefinition = BuiltinFunctions.WEIGHTED_AVG
    override fun newAggregator(argumentTypes: List<Type>) = object : Aggregator {
        private val sumWeightedValues = Sum.newAggregator(listOf(Type.NUMERIC))
        private val sumWeights = Sum.newAggregator(listOf(Type.NUMERIC))
        private val argumentArray = arrayOf(TypedValue())
        private val firstOutValue = TypedValue()
        private val secondOutValue = TypedValue()
        override fun reset() {
            sumWeightedValues.reset()
            sumWeights.reset()
        }

        override fun addRow(arguments: Array<TypedValue>) {
            argumentArray[0].numericValue = arguments[0].numericValue * arguments[1].numericValue
            sumWeightedValues.addRow(argumentArray)
            argumentArray[0].numericValue = arguments[1].numericValue
            sumWeights.addRow(argumentArray)
        }

        override fun writeResultTo(outValue: TypedValue) {
            sumWeightedValues.writeResultTo(firstOutValue)
            sumWeights.writeResultTo(secondOutValue)
            outValue.numericValue = firstOutValue.numericValue / secondOutValue.numericValue
        }
    }

    override fun newAggregator(
        argumentExpressions: List<PhysicalExpression>,
        argumentTypes: List<Type>
    ) = object : AggregatorV2 {
        private val valueExpr = argumentExpressions[0] as NumericPhysicalExpression
        private val weightExpr = argumentExpressions[1] as NumericPhysicalExpression
        private var sumWeightedValues = 0.0
        private var sumWeights = 0.0
        override fun reset() {
            sumWeightedValues = 0.0
            sumWeights = 0.0
        }

        override fun addRow(row: Row) {
            val value = valueExpr.evaluateAsNumeric(row)
            val weight = weightExpr.evaluateAsNumeric(row)
            sumWeightedValues += value * weight
            sumWeights += weight
        }

        override fun getNumericResult(): Double {
            return sumWeightedValues / sumWeights
        }
    }
}