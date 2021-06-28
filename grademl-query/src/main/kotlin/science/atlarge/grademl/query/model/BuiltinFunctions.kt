package science.atlarge.grademl.query.model

import science.atlarge.grademl.query.language.FunctionDefinition
import science.atlarge.grademl.query.language.Type

object BuiltinFunctions : Iterable<FunctionDefinition> {

    override fun iterator() = listOf(COUNT, COUNT_IF, MIN, MAX, SUM, AVG, WEIGHTED_AVG).iterator()

    object COUNT : FunctionDefinition {
        override val functionName = "COUNT"
        override val isAggregatingFunction = true

        override fun checkArgumentCount(argCount: Int) {
            require(argCount == 1) { "$functionName requires 1 argument" }
        }

        override fun checkArgumentTypes(argTypes: List<Type>) {
            // Any type is accepted
        }

        override fun resolveType(argTypes: List<Type>): Type {
            return Type.NUMERIC
        }
    }

    object COUNT_IF : FunctionDefinition {
        override val functionName = "COUNT_IF"
        override val isAggregatingFunction = true

        override fun checkArgumentCount(argCount: Int) {
            require(argCount == 1) { "$functionName requires 1 argument" }
        }

        override fun checkArgumentTypes(argTypes: List<Type>) {
            require(argTypes[0] == Type.BOOLEAN) { "$functionName requires a BOOLEAN argument" }
        }

        override fun resolveType(argTypes: List<Type>): Type {
            return Type.NUMERIC
        }
    }

    object MIN : FunctionDefinition {
        override val functionName = "MIN"
        override val isAggregatingFunction = true

        override fun checkArgumentCount(argCount: Int) {
            require(argCount == 1) { "$functionName requires 1 argument" }
        }

        override fun checkArgumentTypes(argTypes: List<Type>) {
            // Any type is accepted
        }

        override fun resolveType(argTypes: List<Type>): Type {
            return argTypes[0]
        }
    }

    object MAX : FunctionDefinition {
        override val functionName = "MAX"
        override val isAggregatingFunction = true

        override fun checkArgumentCount(argCount: Int) {
            require(argCount == 1) { "$functionName requires 1 argument" }
        }

        override fun checkArgumentTypes(argTypes: List<Type>) {
            // Any type is accepted
        }

        override fun resolveType(argTypes: List<Type>): Type {
            return argTypes[0]
        }
    }

    object SUM : FunctionDefinition {
        override val functionName = "SUM"
        override val isAggregatingFunction = true

        override fun checkArgumentCount(argCount: Int) {
            require(argCount == 1) { "$functionName requires 1 argument" }
        }

        override fun checkArgumentTypes(argTypes: List<Type>) {
            require(argTypes[0] == Type.NUMERIC) { "$functionName requires a NUMERIC argument" }
        }

        override fun resolveType(argTypes: List<Type>): Type {
            return Type.NUMERIC
        }
    }

    object AVG : FunctionDefinition {
        override val functionName = "AVG"
        override val isAggregatingFunction = true

        override fun checkArgumentCount(argCount: Int) {
            require(argCount == 1) { "$functionName requires 1 argument" }
        }

        override fun checkArgumentTypes(argTypes: List<Type>) {
            require(argTypes[0] == Type.NUMERIC) { "$functionName requires a NUMERIC argument" }
        }

        override fun resolveType(argTypes: List<Type>): Type {
            return Type.NUMERIC
        }
    }

    object WEIGHTED_AVG : FunctionDefinition {
        override val functionName = "WEIGHTED_AVG"
        override val isAggregatingFunction = true

        override fun checkArgumentCount(argCount: Int) {
            require(argCount == 2) { "$functionName requires 2 arguments (value, weight)" }
        }

        override fun checkArgumentTypes(argTypes: List<Type>) {
            require(argTypes[0] == Type.NUMERIC) { "First argument of $functionName (\"value\") must be NUMERIC" }
            require(argTypes[1] == Type.NUMERIC) { "Second argument of $functionName (\"weight\") must be NUMERIC" }
        }

        override fun resolveType(argTypes: List<Type>): Type {
            return Type.NUMERIC
        }
    }

}
