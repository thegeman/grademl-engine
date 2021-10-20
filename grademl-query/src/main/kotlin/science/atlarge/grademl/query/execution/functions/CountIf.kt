package science.atlarge.grademl.query.execution.functions

import science.atlarge.grademl.query.execution.*
import science.atlarge.grademl.query.language.FunctionDefinition
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.BuiltinFunctions
import science.atlarge.grademl.query.model.TypedValue
import science.atlarge.grademl.query.model.v2.Row

object CountIf : AggregatingFunctionImplementation {
    override val definition: FunctionDefinition = BuiltinFunctions.COUNT_IF
    override fun newAggregator(argumentTypes: List<Type>) = object : Aggregator {
        private var count = 0
        override fun reset() {
            count = 0
        }

        override fun addRow(arguments: Array<TypedValue>) {
            if (arguments[0].booleanValue) count++
        }

        override fun writeResultTo(outValue: TypedValue) {
            outValue.numericValue = count.toDouble()
        }
    }

    override fun newAggregator(
        argumentExpressions: List<PhysicalExpression>,
        argumentTypes: List<Type>
    ) = object : AggregatorV2 {
        private val condition = argumentExpressions[0] as BooleanPhysicalExpression
        private var count = 0
        override fun reset() {
            count = 0
        }

        override fun addRow(row: Row) {
            if (condition.evaluateAsBoolean(row)) count++
        }

        override fun getNumericResult(): Double {
            return count.toDouble()
        }
    }
}