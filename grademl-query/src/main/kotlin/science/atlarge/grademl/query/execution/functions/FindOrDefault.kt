package science.atlarge.grademl.query.execution.functions

import science.atlarge.grademl.query.execution.AggregatingFunctionImplementation
import science.atlarge.grademl.query.execution.Aggregator
import science.atlarge.grademl.query.language.FunctionDefinition
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.BuiltinFunctions
import science.atlarge.grademl.query.model.TypedValue

object FindOrDefault : AggregatingFunctionImplementation {
    override val definition: FunctionDefinition = BuiltinFunctions.FIND_OR_DEFAULT
    override fun newAggregator(argumentTypes: List<Type>) = object : Aggregator {
        private var valueFound = false
        private var firstRow = true
        private val value = TypedValue()

        override fun reset() {
            valueFound = false
            firstRow = true
            value.clear()
        }

        override fun addRow(arguments: Array<TypedValue>) {
            if (!valueFound && arguments[0].booleanValue) {
                arguments[1].copyTo(value)
                valueFound = true
            } else if (!valueFound && firstRow) {
                arguments[2].copyTo(value)
                firstRow = false
            }
        }

        override fun writeResultTo(outValue: TypedValue) {
            value.copyTo(outValue)
        }
    }
}