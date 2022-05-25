package science.atlarge.grademl.query.execution.functions

import science.atlarge.grademl.query.PathUtils
import science.atlarge.grademl.query.execution.MappingFunctionImplementation
import science.atlarge.grademl.query.execution.NumericPhysicalExpression
import science.atlarge.grademl.query.execution.PhysicalExpression
import science.atlarge.grademl.query.execution.StringPhysicalExpression
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.BuiltinFunctions
import science.atlarge.grademl.query.model.Row

object PathDepth : MappingFunctionImplementation {

    override val definition = BuiltinFunctions.PATH_DEPTH

    override fun toPhysicalExpression(
        arguments: List<PhysicalExpression>,
        argumentTypes: List<Type>
    ): NumericPhysicalExpression {
        definition.checkArgumentCount(arguments.size)
        definition.checkArgumentTypes(argumentTypes)

        return object : NumericPhysicalExpression {
            private val expr = arguments[0] as StringPhysicalExpression
            override fun evaluateAsNumeric(row: Row): Double {
                val path = expr.evaluateAsString(row)
                if (path.isEmpty()) return 0.0
                val isAbsolute = path[0] in PathUtils.SEPARATORS
                val numSeparators = path.count { it in PathUtils.SEPARATORS }
                return ((if (isAbsolute) -1 else 0) + numSeparators).toDouble()
            }
        }
    }

}