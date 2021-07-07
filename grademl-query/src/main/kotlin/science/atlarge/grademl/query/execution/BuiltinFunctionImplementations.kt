package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.execution.functions.*
import science.atlarge.grademl.query.language.FunctionDefinition

object BuiltinFunctionImplementations {

    private val allImplementations = listOf(Count, CountIf, Min, Max, Sum, Average, WeightedAverage)
        .associateBy { it.definition }

    fun from(definition: FunctionDefinition): FunctionImplementation {
        return allImplementations[definition] ?: throw IllegalArgumentException(
            "No implementation defined for function ${definition.functionName}"
        )
    }

}