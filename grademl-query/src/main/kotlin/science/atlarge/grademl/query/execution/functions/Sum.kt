package science.atlarge.grademl.query.execution.functions

import science.atlarge.grademl.query.execution.*
import science.atlarge.grademl.query.language.FunctionDefinition
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.BuiltinFunctions
import science.atlarge.grademl.query.model.TypedValue
import science.atlarge.grademl.query.model.v2.Row

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

    override fun newAggregator(
        argumentExpressions: List<PhysicalExpression>,
        argumentTypes: List<Type>
    ) = object : AggregatorV2 {
        private val expr = argumentExpressions[0] as NumericPhysicalExpression
        private var sum = 0.0
        override fun reset() {
            sum = 0.0
        }

        override fun addRow(row: Row) {
            sum += expr.evaluateAsNumeric(row)
        }

        override fun getNumericResult(): Double {
            return sum
        }
    }
}