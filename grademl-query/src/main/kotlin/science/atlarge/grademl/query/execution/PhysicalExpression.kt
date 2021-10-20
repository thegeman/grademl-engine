package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.execution.operators.IntTypes
import science.atlarge.grademl.query.execution.operators.IntTypes.toInt
import science.atlarge.grademl.query.language.*
import science.atlarge.grademl.query.model.TypedValue
import science.atlarge.grademl.query.model.v2.Row

sealed interface PhysicalExpression

interface BooleanPhysicalExpression : PhysicalExpression {
    fun evaluateAsBoolean(row: Row): Boolean

    companion object {
        val ALWAYS_TRUE = object : BooleanPhysicalExpression {
            override fun evaluateAsBoolean(row: Row) = true
        }
        val ALWAYS_FALSE = object : BooleanPhysicalExpression {
            override fun evaluateAsBoolean(row: Row) = false
        }
    }
}

interface NumericPhysicalExpression : PhysicalExpression {
    fun evaluateAsNumeric(row: Row): Double
}

interface StringPhysicalExpression : PhysicalExpression {
    fun evaluateAsString(row: Row): String
}

fun Expression.toPhysicalExpression(): PhysicalExpression {
    val visitor = PhysicalExpressionConverter()
    accept(visitor)
    return visitor.result
}

private class PhysicalExpressionConverter : ExpressionVisitor {
    lateinit var result: PhysicalExpression
        private set

    private fun Expression.convert(): PhysicalExpression {
        accept(this@PhysicalExpressionConverter)
        return result
    }

    override fun visit(e: BooleanLiteral) {
        result = object : BooleanPhysicalExpression {
            override fun evaluateAsBoolean(row: Row): Boolean {
                return e.value
            }
        }
    }

    override fun visit(e: NumericLiteral) {
        result = object : NumericPhysicalExpression {
            override fun evaluateAsNumeric(row: Row): Double {
                return e.value
            }
        }
    }

    override fun visit(e: StringLiteral) {
        result = object : StringPhysicalExpression {
            override fun evaluateAsString(row: Row): String {
                return e.value
            }
        }
    }

    override fun visit(e: ColumnLiteral) {
        result = when (e.type) {
            Type.UNDEFINED -> throw IllegalArgumentException("Cannot evaluate UNDEFINED ColumnLiteral")
            Type.BOOLEAN -> object : BooleanPhysicalExpression {
                override fun evaluateAsBoolean(row: Row): Boolean {
                    return row.getBoolean(e.columnIndex)
                }
            }
            Type.NUMERIC -> object : NumericPhysicalExpression {
                override fun evaluateAsNumeric(row: Row): Double {
                    return row.getNumeric(e.columnIndex)
                }
            }
            Type.STRING -> object : StringPhysicalExpression {
                override fun evaluateAsString(row: Row): String {
                    return row.getString(e.columnIndex)
                }
            }
        }
    }

    override fun visit(e: UnaryExpression) {
        val innerExpression = e.expr.convert()
        result = when (e.op) {
            UnaryOp.NOT -> {
                innerExpression as BooleanPhysicalExpression
                object : BooleanPhysicalExpression {
                    override fun evaluateAsBoolean(row: Row): Boolean {
                        return !innerExpression.evaluateAsBoolean(row)
                    }
                }
            }
        }
    }

    override fun visit(e: BinaryExpression) {
        val leftExpression = e.lhs.convert()
        val rightExpression = e.rhs.convert()
        result = when (e.op) {
            BinaryOp.ADD -> {
                leftExpression as NumericPhysicalExpression
                rightExpression as NumericPhysicalExpression
                object : NumericPhysicalExpression {
                    override fun evaluateAsNumeric(row: Row): Double {
                        return leftExpression.evaluateAsNumeric(row) + rightExpression.evaluateAsNumeric(row)
                    }
                }
            }
            BinaryOp.SUBTRACT -> {
                leftExpression as NumericPhysicalExpression
                rightExpression as NumericPhysicalExpression
                object : NumericPhysicalExpression {
                    override fun evaluateAsNumeric(row: Row): Double {
                        return leftExpression.evaluateAsNumeric(row) - rightExpression.evaluateAsNumeric(row)
                    }
                }
            }
            BinaryOp.MULTIPLY -> {
                leftExpression as NumericPhysicalExpression
                rightExpression as NumericPhysicalExpression
                object : NumericPhysicalExpression {
                    override fun evaluateAsNumeric(row: Row): Double {
                        return leftExpression.evaluateAsNumeric(row) * rightExpression.evaluateAsNumeric(row)
                    }
                }
            }
            BinaryOp.DIVIDE -> {
                leftExpression as NumericPhysicalExpression
                rightExpression as NumericPhysicalExpression
                object : NumericPhysicalExpression {
                    override fun evaluateAsNumeric(row: Row): Double {
                        return leftExpression.evaluateAsNumeric(row) / rightExpression.evaluateAsNumeric(row)
                    }
                }
            }
            BinaryOp.AND -> {
                leftExpression as BooleanPhysicalExpression
                rightExpression as BooleanPhysicalExpression
                object : BooleanPhysicalExpression {
                    override fun evaluateAsBoolean(row: Row): Boolean {
                        return leftExpression.evaluateAsBoolean(row) && rightExpression.evaluateAsBoolean(row)
                    }
                }
            }
            BinaryOp.OR -> {
                leftExpression as BooleanPhysicalExpression
                rightExpression as BooleanPhysicalExpression
                object : BooleanPhysicalExpression {
                    override fun evaluateAsBoolean(row: Row): Boolean {
                        return leftExpression.evaluateAsBoolean(row) || rightExpression.evaluateAsBoolean(row)
                    }
                }
            }
            BinaryOp.EQUAL -> {
                when (leftExpression) {
                    is BooleanPhysicalExpression -> {
                        rightExpression as BooleanPhysicalExpression
                        object : BooleanPhysicalExpression {
                            override fun evaluateAsBoolean(row: Row): Boolean {
                                return leftExpression.evaluateAsBoolean(row) == rightExpression.evaluateAsBoolean(row)
                            }
                        }
                    }
                    is NumericPhysicalExpression -> {
                        rightExpression as NumericPhysicalExpression
                        object : BooleanPhysicalExpression {
                            override fun evaluateAsBoolean(row: Row): Boolean {
                                return leftExpression.evaluateAsNumeric(row) == rightExpression.evaluateAsNumeric(row)
                            }
                        }
                    }
                    is StringPhysicalExpression -> {
                        rightExpression as StringPhysicalExpression
                        object : BooleanPhysicalExpression {
                            override fun evaluateAsBoolean(row: Row): Boolean {
                                return leftExpression.evaluateAsString(row) == rightExpression.evaluateAsString(row)
                            }
                        }
                    }
                }
            }
            BinaryOp.NOT_EQUAL -> {
                when (leftExpression) {
                    is BooleanPhysicalExpression -> {
                        rightExpression as BooleanPhysicalExpression
                        object : BooleanPhysicalExpression {
                            override fun evaluateAsBoolean(row: Row): Boolean {
                                return leftExpression.evaluateAsBoolean(row) != rightExpression.evaluateAsBoolean(row)
                            }
                        }
                    }
                    is NumericPhysicalExpression -> {
                        rightExpression as NumericPhysicalExpression
                        object : BooleanPhysicalExpression {
                            override fun evaluateAsBoolean(row: Row): Boolean {
                                return leftExpression.evaluateAsNumeric(row) != rightExpression.evaluateAsNumeric(row)
                            }
                        }
                    }
                    is StringPhysicalExpression -> {
                        rightExpression as StringPhysicalExpression
                        object : BooleanPhysicalExpression {
                            override fun evaluateAsBoolean(row: Row): Boolean {
                                return leftExpression.evaluateAsString(row) != rightExpression.evaluateAsString(row)
                            }
                        }
                    }
                }
            }
            BinaryOp.APPROX_EQUAL -> {
                leftExpression as StringPhysicalExpression
                rightExpression as StringPhysicalExpression
                object : BooleanPhysicalExpression {
                    override fun evaluateAsBoolean(row: Row): Boolean {
                        return matchPathsWithWildcards(
                            leftExpression.evaluateAsString(row),
                            rightExpression.evaluateAsString(row)
                        )
                    }
                }
            }
            BinaryOp.NOT_APPROX_EQUAL -> {
                leftExpression as StringPhysicalExpression
                rightExpression as StringPhysicalExpression
                object : BooleanPhysicalExpression {
                    override fun evaluateAsBoolean(row: Row): Boolean {
                        return !matchPathsWithWildcards(
                            leftExpression.evaluateAsString(row),
                            rightExpression.evaluateAsString(row)
                        )
                    }
                }
            }
            BinaryOp.GREATER -> {
                leftExpression as NumericPhysicalExpression
                rightExpression as NumericPhysicalExpression
                object : BooleanPhysicalExpression {
                    override fun evaluateAsBoolean(row: Row): Boolean {
                        return leftExpression.evaluateAsNumeric(row) > rightExpression.evaluateAsNumeric(row)
                    }
                }
            }
            BinaryOp.GREATER_EQUAL -> {
                leftExpression as NumericPhysicalExpression
                rightExpression as NumericPhysicalExpression
                object : BooleanPhysicalExpression {
                    override fun evaluateAsBoolean(row: Row): Boolean {
                        return leftExpression.evaluateAsNumeric(row) >= rightExpression.evaluateAsNumeric(row)
                    }
                }
            }
            BinaryOp.SMALLER -> {
                leftExpression as NumericPhysicalExpression
                rightExpression as NumericPhysicalExpression
                object : BooleanPhysicalExpression {
                    override fun evaluateAsBoolean(row: Row): Boolean {
                        return leftExpression.evaluateAsNumeric(row) < rightExpression.evaluateAsNumeric(row)
                    }
                }
            }
            BinaryOp.SMALLER_EQUAL -> {
                leftExpression as NumericPhysicalExpression
                rightExpression as NumericPhysicalExpression
                object : BooleanPhysicalExpression {
                    override fun evaluateAsBoolean(row: Row): Boolean {
                        return leftExpression.evaluateAsNumeric(row) <= rightExpression.evaluateAsNumeric(row)
                    }
                }
            }
        }
    }

    override fun visit(e: FunctionCallExpression) {
        TODO("Not yet implemented")
    }

    override fun visit(e: CustomExpression) {
        // TODO: Avoid using TypedValues
        val args = e.arguments.map { it.convert() }
        val argTypes = e.arguments.map { it.type.toInt() }
        result = when (e.type) {
            Type.UNDEFINED -> throw IllegalArgumentException("Cannot evaluate UNDEFINED CustomExpression")
            Type.BOOLEAN -> object : BooleanPhysicalExpression {
                val scratchArgs = e.arguments.map { TypedValue() }
                val scratchOut = TypedValue()
                override fun evaluateAsBoolean(row: Row): Boolean {
                    argTypes.forEachIndexed { index, type ->
                        when (type) {
                            IntTypes.TYPE_BOOLEAN -> scratchArgs[index].booleanValue =
                                (args[index] as BooleanPhysicalExpression).evaluateAsBoolean(row)
                            IntTypes.TYPE_NUMERIC -> scratchArgs[index].numericValue =
                                (args[index] as NumericPhysicalExpression).evaluateAsNumeric(row)
                            IntTypes.TYPE_STRING -> scratchArgs[index].stringValue =
                                (args[index] as StringPhysicalExpression).evaluateAsString(row)
                        }
                    }
                    return e.evalFunction(scratchArgs, scratchOut).booleanValue
                }
            }
            Type.NUMERIC -> object : NumericPhysicalExpression {
                val scratchArgs = e.arguments.map { TypedValue() }
                val scratchOut = TypedValue()
                override fun evaluateAsNumeric(row: Row): Double {
                    argTypes.forEachIndexed { index, type ->
                        when (type) {
                            IntTypes.TYPE_BOOLEAN -> scratchArgs[index].booleanValue =
                                (args[index] as BooleanPhysicalExpression).evaluateAsBoolean(row)
                            IntTypes.TYPE_NUMERIC -> scratchArgs[index].numericValue =
                                (args[index] as NumericPhysicalExpression).evaluateAsNumeric(row)
                            IntTypes.TYPE_STRING -> scratchArgs[index].stringValue =
                                (args[index] as StringPhysicalExpression).evaluateAsString(row)
                        }
                    }
                    return e.evalFunction(scratchArgs, scratchOut).numericValue
                }
            }
            Type.STRING -> object : StringPhysicalExpression {
                val scratchArgs = e.arguments.map { TypedValue() }
                val scratchOut = TypedValue()
                override fun evaluateAsString(row: Row): String {
                    argTypes.forEachIndexed { index, type ->
                        when (type) {
                            IntTypes.TYPE_BOOLEAN -> scratchArgs[index].booleanValue =
                                (args[index] as BooleanPhysicalExpression).evaluateAsBoolean(row)
                            IntTypes.TYPE_NUMERIC -> scratchArgs[index].numericValue =
                                (args[index] as NumericPhysicalExpression).evaluateAsNumeric(row)
                            IntTypes.TYPE_STRING -> scratchArgs[index].stringValue =
                                (args[index] as StringPhysicalExpression).evaluateAsString(row)
                        }
                    }
                    return e.evalFunction(scratchArgs, scratchOut).stringValue
                }
            }
        }
    }

    private fun matchPathsWithWildcards(l: String, r: String): Boolean {
        return when {
            '*' in l -> {
                require('*' !in r) { "Approximate matching between two paths with wildcards is not supported" }
                l.split('*').joinToString(separator = ".*") { Regex.escape(it) }.toRegex().matches(r)
            }
            '*' in r -> matchPathsWithWildcards(r, l)
            else -> r == l
        }
    }
}