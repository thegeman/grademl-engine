package science.atlarge.grademl.query.execution.functions

import science.atlarge.grademl.query.execution.AggregatingFunctionImplementation
import science.atlarge.grademl.query.execution.Aggregator
import science.atlarge.grademl.query.language.FunctionDefinition
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.BuiltinFunctions
import science.atlarge.grademl.query.model.TypedValue

object Sum : AggregatingFunctionImplementation {
    override val definition: FunctionDefinition = BuiltinFunctions.SUM
    override fun newAggregator(argumentTypes: List<Type>) = object : Aggregator {
        private var sum = 0.0
        override fun reset() {
            sum = 0.0
        }

        override fun addRow(arguments: Array<TypedValue>) {
            sum += arguments[0].numericValue
        }

        override fun writeResultTo(outValue: TypedValue) {
            outValue.numericValue = sum
        }
    }
}