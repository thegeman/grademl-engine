package science.atlarge.grademl.query.plan

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

        private fun StringBuilder.indent(): StringBuilder {
            repeat(currentDepth) { stringBuilder.append("   ") }
            return this
        }

        override fun visit(aggregatePlan: AggregatePlan) {
            // Append one line with top-level description
            stringBuilder.indent()
                .append("Aggregate[")
                .append(aggregatePlan.nodeId)
                .append("] - ")
            if (aggregatePlan.groupExpressions.isNotEmpty()) {
                stringBuilder.append("Group by: [")
                var isFirst = true
                for (g in aggregatePlan.groupExpressions) {
                    if (!isFirst) stringBuilder.append(", ")
                    stringBuilder.append(g)
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
                stringBuilder.indent().append("    Column ")
                    .append(aggregatePlan.schema.columns[i].identifier)
                    .append(" = ")
                    .append(aggregatePlan.aggregateExpressions[i])
                    .appendLine()
            }
            // Explain input node
            currentDepth++
            aggregatePlan.input.accept(this)
            currentDepth--
        }

        override fun visit(filterPlan: FilterPlan) {
            // Append one line with top-level description
            stringBuilder.indent()
                .append("Filter[")
                .append(filterPlan.nodeId)
                .append("] - Condition: ")
                .append(filterPlan.condition)
                .appendLine()
            // Explain input node
            currentDepth++
            filterPlan.input.accept(this)
            currentDepth--
        }

        override fun visit(projectPlan: ProjectPlan) {
            // Append one line with top-level description
            stringBuilder.indent()
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
                stringBuilder.indent().append("    Column ")
                    .append(projectPlan.schema.columns[i].identifier)
                    .append(" = ")
                    .append(projectPlan.columnExpressions[i])
                    .appendLine()
            }
            // Explain input node
            currentDepth++
            projectPlan.input.accept(this)
            currentDepth--
        }

        override fun visit(scanTablePlan: ScanTablePlan) {
            // Append one line with top-level description
            stringBuilder.indent()
                .append("ScanTable[")
                .append(scanTablePlan.nodeId)
                .append("] - Table: ")
                .append(scanTablePlan.tableName)
                .appendLine()
            // Append an additional line listing column names
            stringBuilder.indent()
                .append("    Columns: [")
            var isFirst = true
            for (c in scanTablePlan.schema.columns) {
                if (!isFirst) stringBuilder.append(", ")
                stringBuilder.append(c.identifier)
                isFirst = false
            }
            stringBuilder.append(']')
                .appendLine()
        }

        override fun visit(temporalJoinPlan: TemporalJoinPlan) {
            // Append one line with top-level description
            stringBuilder.indent()
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