package science.atlarge.grademl.query.execution.functions

import science.atlarge.grademl.query.execution.AggregatingFunctionImplementation
import science.atlarge.grademl.query.execution.Aggregator
import science.atlarge.grademl.query.language.FunctionDefinition
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.BuiltinFunctions
import science.atlarge.grademl.query.model.TypedValue

object Count : AggregatingFunctionImplementation {
    override val definition: FunctionDefinition = BuiltinFunctions.COUNT
    override fun newAggregator(argumentTypes: List<Type>) = object : Aggregator {
        private var count = 0
        override fun reset() {
            count = 0
        }

        override fun addRow(arguments: Array<TypedValue>) {
            count++
        }

        override fun writeResultTo(outValue: TypedValue) {
            outValue.numericValue = count.toDouble()
        }
    }
}