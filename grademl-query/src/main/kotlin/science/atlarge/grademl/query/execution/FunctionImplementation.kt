package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.language.FunctionDefinition
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.TypedValue
import science.atlarge.grademl.query.model.v2.Row

sealed interface FunctionImplementation {
    val definition: FunctionDefinition
}

interface MappingFunctionImplementation : FunctionImplementation {
    fun computeValue(arguments: List<TypedValue>, argumentCount: Int, outValue: TypedValue)
}

interface AggregatingFunctionImplementation : FunctionImplementation {
    fun newAggregator(argumentTypes: List<Type>): Aggregator
    fun newAggregator(argumentExpressions: List<PhysicalExpression>, argumentTypes: List<Type>): AggregatorV2
}

interface Aggregator {
    fun reset()
    fun addRow(arguments: Array<TypedValue>)
    fun writeResultTo(outValue: TypedValue)
}

interface AggregatorV2 {
    fun reset()
    fun addRow(row: Row)
    fun getBooleanResult(): Boolean {
        throw UnsupportedOperationException("Aggregator does not produce a BOOLEAN value")
    }

    fun getNumericResult(): Double {
        throw UnsupportedOperationException("Aggregator does not produce a NUMERIC value")
    }

    fun getStringResult(): String {
        throw UnsupportedOperationException("Aggregator does not produce a STRING value")
    }
}