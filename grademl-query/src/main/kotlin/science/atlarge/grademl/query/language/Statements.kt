package science.atlarge.grademl.query.language

sealed interface Statement : ASTNode
sealed interface Clause : ASTNode

class SelectStatement(
    val from: FromClause,
    val where: WhereClause?,
    val groupBy: GroupByClause?,
    val select: SelectClause,
    val orderBy: OrderByClause?,
    val limit: LimitClause?
) : Statement {
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
}

class FromClause(val tableNames: List<String>, val aliases: List<String>) : Clause {
    init { require(tableNames.size == aliases.size) }
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
}

class WhereClause(val conditionExpression: Expression) : Clause {
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
}

class GroupByClause(val columns: List<ColumnLiteral>) : Clause {
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
}

class SelectClause(val columnExpressions: List<Expression>, val columnAliases: List<String?>) : Clause {
    init { require(columnExpressions.size == columnAliases.size) }
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
}

class OrderByClause(val columns: List<ColumnLiteral>) : Clause {
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
}

class LimitClause(val limitFirst: Int?, val limitLast: Int?) : Clause {
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
}

class CreateTableStatement(val tableName: String, val tableDefinition: SelectStatement) : Statement {
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
}

class DeleteTableStatement(val tableName: String) : Statement {
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
}

class CacheTableStatement(val tableName: String) : Statement {
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
}

class DropTableFromCacheStatement(val tableName: String) : Statement {
    override fun accept(visitor: ASTVisitor) { visitor.visit(this) }
}