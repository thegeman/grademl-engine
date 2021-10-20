package science.atlarge.grademl.query.execution.functions

import science.atlarge.grademl.query.execution.AggregatingFunctionImplementation
import science.atlarge.grademl.query.execution.Aggregator
import science.atlarge.grademl.query.execution.AggregatorV2
import science.atlarge.grademl.query.execution.PhysicalExpression
import science.atlarge.grademl.query.language.FunctionDefinition
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.BuiltinFunctions
import science.atlarge.grademl.query.model.TypedValue
import science.atlarge.grademl.query.model.v2.Row

object Average : AggregatingFunctionImplementation {
    override val definition: FunctionDefinition = BuiltinFunctions.AVG
    override fun newAggregator(argumentTypes: List<Type>) = object : Aggregator {
        private val sum = Sum.newAggregator(argumentTypes)
        private val count = Count.newAggregator(argumentTypes)
        private val sumValue = TypedValue()
        private val countValue = TypedValue()
        override fun reset() {
            sum.reset()
            count.reset()
        }

        override fun addRow(arguments: Array<TypedValue>) {
            sum.addRow(arguments)
            count.addRow(arguments)
        }

        override fun writeResultTo(outValue: TypedValue) {
            sum.writeResultTo(sumValue)
            count.writeResultTo(countValue)
            outValue.numericValue = sumValue.numericValue / countValue.numericValue
        }
    }

    override fun newAggregator(
        argumentExpressions: List<PhysicalExpression>,
        argumentTypes: List<Type>
    ) = object : AggregatorV2 {
        private val sum = Sum.newAggregator(argumentExpressions, argumentTypes)
        private val count = Count.newAggregator(argumentExpressions, argumentTypes)

        override fun reset() {
            sum.reset()
            count.reset()
        }

        override fun addRow(row: Row) {
            sum.addRow(row)
            count.addRow(row)
        }

        override fun getNumericResult(): Double {
            return sum.getNumericResult() / count.getNumericResult()
        }
    }
}