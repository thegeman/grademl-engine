package science.atlarge.grademl.query.plan

import science.atlarge.grademl.query.language.prettyPrint
import science.atlarge.grademl.query.plan.logical.*

object ExplainLogicalPlan {

    fun explain(queryPlan: LogicalQueryPlan): String {
        val visitor = Visitor()
        queryPlan.accept(visitor)
        return visitor.toString()
    }

    private class Visitor : LogicalQueryPlanVisitor {

        private val stringBuilder = StringBuilder()
        private var currentDepth = 0

        private fun StringBuilder.indentSummary(): StringBuilder {
            repeat(currentDepth) { stringBuilder.append("   ") }
            return this
        }

        private fun StringBuilder.indentDetail(): StringBuilder {
            return indentSummary().append("      ")
        }

        override fun visit(aggregatePlan: AggregatePlan) {
            // Append one line with top-level description
            stringBuilder.indentSummary()
                .append("Aggregate[")
                .append(aggregatePlan.nodeId)
                .append("] - ")
            if (aggregatePlan.groupExpressions.isNotEmpty()) {
                stringBuilder.append("Group by: [")
                var isFirst = true
                for (g in aggregatePlan.groupExpressions) {
                    if (!isFirst) stringBuilder.append(", ")
                    stringBuilder.append(g.prettyPrint())
                    isFirst = false
                }
                stringBuilder.append("], ")
            }
            stringBuilder.append("Columns: [")
            var isFirst = true
            for (c in aggregatePlan.schema.columns) {
                if (!isFirst) stringBuilder.append(", ")
                stringBuilder.append(c.identifier)
                isFirst = false
            }
            stringBuilder.append(']')
                .appendLine()
            // Append one line per projection expression
            for (i in aggregatePlan.schema.columns.indices) {
                stringBuilder.indentDetail()
                    .append("Column ")
                    .append(aggregatePlan.schema.columns[i].identifier)
                    .append(" = ")
                    .append(aggregatePlan.aggregateExpressions[i].expr.prettyPrint())
                    .appendLine()
            }
            // Explain input node
            currentDepth++
            aggregatePlan.input.accept(this)
            currentDepth--
        }

        override fun visit(filterPlan: FilterPlan) {
            // Append one line with top-level description
            stringBuilder.indentSummary()
                .append("Filter[")
                .append(filterPlan.nodeId)
                .append("] - Condition: ")
                .append(filterPlan.condition.prettyPrint())
                .appendLine()
            // Explain input node
            currentDepth++
            filterPlan.input.accept(this)
            currentDepth--
        }

        override fun visit(projectPlan: ProjectPlan) {
            // Append one line with top-level description
            stringBuilder.indentSummary()
                .append("Project[")
                .append(projectPlan.nodeId)
                .append("] - Columns: [")
            var isFirst = true
            for (c in projectPlan.schema.columns) {
                if (!isFirst) stringBuilder.append(", ")
                stringBuilder.append(c.identifier)
                isFirst = false
            }
            stringBuilder.append(']')
                .appendLine()
            // Append one line per projection expression
            for (i in projectPlan.schema.columns.indices) {
                stringBuilder.indentDetail()
                    .append("Column ")
                    .append(projectPlan.schema.columns[i].identifier)
                    .append(" = ")
                    .append(projectPlan.columnExpressions[i].prettyPrint())
                    .appendLine()
            }
            // Explain input node
            currentDepth++
            projectPlan.input.accept(this)
            currentDepth--
        }

        override fun visit(scanTablePlan: ScanTablePlan) {
            // Append one line with top-level description
            stringBuilder.indentSummary()
                .append("ScanTable[")
                .append(scanTablePlan.nodeId)
                .append("] - Table: ")
                .append(scanTablePlan.tableName)
                .appendLine()
            // Append an additional line listing column names
            stringBuilder.indentDetail()
                .append("Columns: [")
            var isFirst = true
            for (c in scanTablePlan.schema.columns) {
                if (!isFirst) stringBuilder.append(", ")
                stringBuilder.append(c.identifier)
                isFirst = false
            }
            stringBuilder.append(']')
                .appendLine()
        }

        override fun visit(sortPlan: SortPlan) {
            // Append one line with top-level description
            stringBuilder.indentSummary()
                .append("Sort[")
                .append(sortPlan.nodeId)
                .append("] - By: [")
            var isFirst = true
            for (c in sortPlan.sortByColumns) {
                if (!isFirst) stringBuilder.append(", ")
                stringBuilder.append(c.column.columnPath)
                    .append(' ')
                    .append(if (c.ascending) "ASCENDING" else "DESCENDING")
                isFirst = false
            }
            stringBuilder.append(']')
                .appendLine()
            // Explain input node
            currentDepth++
            sortPlan.input.accept(this)
            currentDepth--
        }

        override fun visit(temporalJoinPlan: TemporalJoinPlan) {
            // Append one line with top-level description
            stringBuilder.indentSummary()
                .append("TemporalJoin[")
                .append(temporalJoinPlan.nodeId)
                .append(']')
                .appendLine()
            // Explain input nodes
            currentDepth++
            temporalJoinPlan.leftInput.accept(this)
            temporalJoinPlan.rightInput.accept(this)
            currentDepth--
        }

        override fun toString(): String {
            return stringBuilder.toString()
        }

    }

}