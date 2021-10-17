package science.atlarge.grademl.query.language

import science.atlarge.grademl.query.model.TypedValue

sealed class Expression : ASTNode, Typed {
    override var type = Type.UNDEFINED
    abstract fun clone(): Expression
}

class NamedExpression(val expr: Expression, val name: String)

class BooleanLiteral(val value: Boolean) : Expression() {
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
    override fun clone() = BooleanLiteral(value).also { it.type = type }

    companion object {
        val TRUE = BooleanLiteral(true)
        val FALSE = BooleanLiteral(false)
    }
}

class NumericLiteral(val value: Double): Expression() {
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
    override fun clone() = NumericLiteral(value).also { it.type = type }
}

class StringLiteral(val value: String) : Expression() {
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
    override fun clone() = StringLiteral(value).also { it.type = type }
}

class ColumnLiteral(val columnPath: String) : Expression() {
    val columnName = columnPath.split('.').last()
    var columnIndex = -1

    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
    override fun clone() = ColumnLiteral(columnPath).also {
        it.type = type
        it.columnIndex = columnIndex
    }
}

class UnaryExpression(val expr: Expression, val op: UnaryOp) : Expression() {
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
    override fun clone() = UnaryExpression(expr.clone(), op).also { it.type = type }
    fun copy(newExpr: Expression = expr) = UnaryExpression(newExpr, op).also { it.type = type }
}

enum class UnaryOp {
    NOT
}

class BinaryExpression(val lhs: Expression, val rhs: Expression, val op: BinaryOp) : Expression() {
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
    override fun clone() = BinaryExpression(lhs.clone(), rhs.clone(), op).also { it.type = type }
    fun copy(newLhs: Expression = lhs, newRhs: Expression = rhs) =
        BinaryExpression(newLhs, newRhs, op).also { it.type = type }
}

enum class BinaryOp {
    ADD,
    SUBTRACT,
    MULTIPLY,
    DIVIDE,
    AND,
    OR,
    EQUAL,
    NOT_EQUAL,
    APPROX_EQUAL,
    NOT_APPROX_EQUAL,
    GREATER,
    GREATER_EQUAL,
    SMALLER,
    SMALLER_EQUAL
}

class FunctionCallExpression(val functionName: String, val arguments: List<Expression>) : Expression() {
    private var _functionDefinition: FunctionDefinition? = null
    var functionDefinition: FunctionDefinition
        get() = _functionDefinition!!
        set(value) { _functionDefinition = value }

    var evalFunction: ((args: List<TypedValue>, outValue: TypedValue) -> TypedValue)? = null

    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
    override fun clone() = FunctionCallExpression(functionName, arguments.map { it.clone() }).also {
        it.type = type
        it._functionDefinition = _functionDefinition
        it.evalFunction = evalFunction
    }

    fun copy(newArguments: List<Expression> = arguments) = FunctionCallExpression(functionName, newArguments).also {
        it.type = type
        it._functionDefinition = _functionDefinition
        it.evalFunction = evalFunction
    }
}

class CustomExpression(
    val arguments: List<Expression>,
    val originalExpression: Expression,
    val evalFunction: (args: List<TypedValue>, outValue: TypedValue) -> TypedValue
) : Expression() {
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
    override fun clone() = CustomExpression(arguments.map { it.clone() }, originalExpression.clone(), evalFunction)
        .also { it.type = type }

    fun copy(newArguments: List<Expression> = arguments, newOriginalExpression: Expression = originalExpression) =
        CustomExpression(newArguments, newOriginalExpression, evalFunction).also { it.type = type }
}
