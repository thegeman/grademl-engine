package science.atlarge.grademl.query.language

sealed class Expression : ASTNode, Typed {
    override var type = Type.UNDEFINED
}

class BooleanLiteral(val value: Boolean) : Expression() {
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }

    companion object {
        val TRUE = BooleanLiteral(true)
        val FALSE = BooleanLiteral(false)
    }
}

class NumericLiteral(val value: Double): Expression() {
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
}

class StringLiteral(val value: String) : Expression() {
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
}

class ColumnLiteral(val tableName: String?, val columnName: String) : Expression() {
    val columnPath = if (tableName == null) columnName else "$tableName.$columnName"
    var columnIndex = -1

    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
}

class UnaryExpression(val expr: Expression, val op: UnaryOp) : Expression() {
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
}

enum class UnaryOp {
    NOT
}

class BinaryExpression(val lhs: Expression, val rhs: Expression, val op: BinaryOp) : Expression() {
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
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
    lateinit var functionDefinition: FunctionDefinition
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
}
