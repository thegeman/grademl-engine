package science.atlarge.grademl.query.language

interface FunctionDefinition {
    val functionName: String
    val isAggregatingFunction: Boolean

    fun checkArgumentCount(argCount: Int)
    fun checkArgumentTypes(argTypes: List<Type>)
    fun resolveType(argTypes: List<Type>): Type
}