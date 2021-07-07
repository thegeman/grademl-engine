package science.atlarge.grademl.query.execution.functions

import science.atlarge.grademl.query.execution.AggregatingFunctionImplementation
import science.atlarge.grademl.query.execution.Aggregator
import science.atlarge.grademl.query.language.FunctionDefinition
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.BuiltinFunctions
import science.atlarge.grademl.query.model.TypedValue

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
}