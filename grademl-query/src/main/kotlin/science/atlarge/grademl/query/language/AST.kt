package science.atlarge.grademl.query.language

sealed interface ASTNode {
    fun accept(visitor: ASTVisitor)
}

interface ASTVisitor {
    // Expressions
    fun visit(e: BooleanLiteral)
    fun visit(e: NumericLiteral)
    fun visit(e: StringLiteral)
    fun visit(e: ColumnLiteral)
    fun visit(e: UnaryExpression)
    fun visit(e: BinaryExpression)
    fun visit(e: FunctionCallExpression)

    // Statements
    fun visit(s: SelectStatement)

    // Clauses
    fun visit(c: FromClause)
    fun visit(c: WhereClause)
    fun visit(c: GroupByClause)
    fun visit(c: SelectClause)
    fun visit(c: LimitClause)
}