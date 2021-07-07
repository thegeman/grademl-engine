package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.execution.functions.*
import science.atlarge.grademl.query.language.FunctionDefinition

object BuiltinFunctionImplementations {

    private val allImplementations = listOf(
        // Common aggregation functions
        Count, CountIf, Min, Max, Sum, Average, WeightedAverage,
        // Data reshaping functions
        FindOrDefault,
        // Helper functions for traversing hierarchical models
        IsParentOf, IsChildOf, IsAncestorOf, IsDescendantOf, ParentOf
    ).associateBy { it.definition }

    fun from(definition: FunctionDefinition): FunctionImplementation {
        return allImplementations[definition] ?: throw IllegalArgumentException(
            "No implementation defined for function ${definition.functionName}"
        )
    }

}