package science.atlarge.grademl.query.parsing

import com.github.h0tk3y.betterParse.combinators.*
import com.github.h0tk3y.betterParse.grammar.Grammar
import com.github.h0tk3y.betterParse.grammar.parser
import com.github.h0tk3y.betterParse.lexer.literalToken
import com.github.h0tk3y.betterParse.lexer.regexToken
import com.github.h0tk3y.betterParse.parser.Parser
import science.atlarge.grademl.query.language.*

object QueryGrammar : Grammar<List<Statement>>() {
    // Tokens

    // Keywords
    private val `true` by regexToken("""[Tt][Rr][Uu][Ee]\b""")
    private val `false` by regexToken("""[Ff][Aa][Ll][Ss][Ee]\b""")
    private val not by regexToken("""[Nn][Oo][Tt]\b""")
    private val and by regexToken("""[Aa][Nn][Dd]\b""")
    private val or by regexToken("""[Oo][Rr]\b""")

    private val from by regexToken("""[Ff][Rr][Oo][Mm]\b""")
    private val `as` by regexToken("""[Aa][Ss]\b""")
    private val temporal by regexToken("""[Tt][Ee][Mm][Pp][Oo][Rr][Aa][Ll]\b""")
    private val join by regexToken("""[Jj][Oo][Ii][Nn]\b""")
    private val where by regexToken("""[Ww][Hh][Ee][Rr][Ee]\b""")
    private val group by regexToken("""[Gg][Rr][Oo][Uu][Pp]\b""")
    private val by by regexToken("""[Bb][Yy]\b""")
    private val select by regexToken("""[Ss][Ee][Ll][Ee][Cc][Tt]\b""")
    private val order by regexToken("""[Oo][Rr][Dd][Ee][Rr]\b""")
    private val limit by regexToken("""[Ll][Ii][Mm][Ii][Tt]\b""")
    private val first by regexToken("""[Ff][Ii][Rr][Ss][Tt]\b""")
    private val last by regexToken("""[Ll][Aa][Ss][Tt]\b""")

    // Literals
    private val id by regexToken("""[a-zA-z_]\w*""")
    private val positiveInteger by regexToken("""[0-9]+(?![Ee.])""")
    private val number by regexToken("""[+-]?(\d+\.)?\d+([Ee][+-]?\d+)?""")
    private val quotedString by regexToken(""""[^"]*"""")

    // Other tokens
    private val plus by literalToken("+")
    private val minus by literalToken("-")
    private val star by literalToken("*")
    private val slash by literalToken("/")
    private val leftParen by literalToken("(")
    private val rightParen by literalToken(")")
    private val period by literalToken(".")
    private val comma by literalToken(",")
    private val semicolon by literalToken(";")
    private val equal2 by literalToken("==")
    private val equal by literalToken("=")
    private val notEqual by literalToken("!=")
    private val approxEqual by literalToken("~=")
    private val notApproxEqual by literalToken("!~")
    private val greaterEqual by literalToken(">=")
    private val greater by literalToken(">")
    private val smallerEqual by literalToken("<=")
    private val smaller by literalToken("<")

    // Ignore whitespace
    private val whitespace by regexToken("""\s+""", ignore = true)

    // Grammar

    // Literals
    private val columnLit by (id * optional(-period * id) map { (l, r) ->
        if (r != null) ColumnLiteral("${l.text}.${r.text}") else ColumnLiteral(l.text)
    })

    private val lit by columnLit or
            (positiveInteger or number use { NumericLiteral(text.toDouble()) }) or
            (quotedString use { StringLiteral(text.trim('"')) }) or
            (`true` map { BooleanLiteral.TRUE }) or
            (`false` map { BooleanLiteral.FALSE })

    // Expressions
    private val term: Parser<Expression> by (-leftParen * parser(this::expression) * -rightParen) or
            (-not * parser(this::expression) map { UnaryExpression(it, UnaryOp.NOT) }) or
            (id * -leftParen * separated(parser(this::expression), comma, acceptZero = true)
                    * -rightParen map { (fName, argList) -> FunctionCallExpression(fName.text, argList.terms) }) or
            lit

    private val plusChain by leftAssociative(term, plus or minus) { l, op, r ->
        BinaryExpression(l, r, toBinaryOp(op.text))
    }

    private val timesChain by leftAssociative(plusChain, star or slash) { l, op, r ->
        BinaryExpression(l, r, toBinaryOp(op.text))
    }

    private val comparisonChain by leftAssociative(
        timesChain, equal2 or equal or notEqual or approxEqual or notApproxEqual or
                greaterEqual or greater or smallerEqual or smaller
    ) { l, op, r ->
        BinaryExpression(l, r, toBinaryOp(op.text))
    }

    private val andChain by leftAssociative(comparisonChain, and) { l, op, r ->
        BinaryExpression(l, r, toBinaryOp(op.text))
    }

    private val expression by leftAssociative(andChain, or) { l, op, r -> BinaryExpression(l, r, toBinaryOp(op.text)) }

    // Clauses
    private val fromClause by -from * (
            (separated(id * -`as` * id, temporal * join)
                    use { FromClause(terms.map { it.t1.text }, terms.map { it.t2.text }) }) or
            (id use { FromClause(listOf(text), listOf("")) })
    )

    private val whereClause by (-where * expression map { WhereClause(it) })

    private val groupByClause by (-group * -by * separated(columnLit, comma) map { GroupByClause(it.terms) })

    private val selectClause by (-select * separated(expression * optional(-`as` * id use { text }), comma)
            map { SelectClause(it.terms.map { t -> t.t1 }, it.terms.map { t -> t.t2 }) })

    private val orderByClause by (-order * -by * separated(columnLit, comma) map { OrderByClause(it.terms) })

    private val limitClause by -limit * ((positiveInteger use { LimitClause(text.toInt(), null) }) or
            (-first * positiveInteger * optional(-last * positiveInteger) use {
                LimitClause(
                    t1.text.toInt(),
                    t2?.text?.toInt()
                )
            }) or
            (-last * positiveInteger use { LimitClause(null, text.toInt()) }))

    // Statements
    private val selectStatement by (fromClause * optional(whereClause) * optional(groupByClause) * selectClause *
            optional(orderByClause) * optional(limitClause)) map { (f, w, g, s, o, l) -> SelectStatement(f, w, g, s, o, l) }

    override val rootParser by oneOrMore((selectStatement) * -semicolon)

    private fun toBinaryOp(op: String): BinaryOp {
        return when (op) {
            "+" -> BinaryOp.ADD
            "-" -> BinaryOp.SUBTRACT
            "*" -> BinaryOp.MULTIPLY
            "/" -> BinaryOp.DIVIDE
            "AND" -> BinaryOp.AND
            "OR" -> BinaryOp.OR
            "==" -> BinaryOp.EQUAL
            "=" -> BinaryOp.EQUAL
            "!=" -> BinaryOp.NOT_EQUAL
            "~=" -> BinaryOp.APPROX_EQUAL
            "!~" -> BinaryOp.NOT_APPROX_EQUAL
            ">" -> BinaryOp.GREATER
            ">=" -> BinaryOp.GREATER_EQUAL
            "<" -> BinaryOp.SMALLER
            "<=" -> BinaryOp.SMALLER_EQUAL
            else -> throw IllegalArgumentException()
        }
    }

}