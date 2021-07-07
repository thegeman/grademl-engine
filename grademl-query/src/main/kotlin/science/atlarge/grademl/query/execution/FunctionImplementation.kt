package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.language.FunctionDefinition
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.TypedValue

sealed interface FunctionImplementation {
    val definition: FunctionDefinition
}

interface MappingFunctionImplementation : FunctionImplementation {
    fun computeValue(arguments: List<TypedValue>, outValue: TypedValue)
}

interface AggregatingFunctionImplementation : FunctionImplementation {
    fun newAggregator(argumentTypes: List<Type>): Aggregator
}

interface Aggregator {
    fun reset()
    fun addRow(arguments: Array<TypedValue>)
    fun writeResultTo(outValue: TypedValue)
}