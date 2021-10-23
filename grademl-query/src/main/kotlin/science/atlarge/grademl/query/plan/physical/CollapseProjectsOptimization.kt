package science.atlarge.grademl.query.plan.physical

import science.atlarge.grademl.query.analysis.ColumnReplacementPass
import science.atlarge.grademl.query.language.NamedExpression

object CollapseProjectsOptimization : OptimizationStrategy, PhysicalQueryPlanRewriter {

    override fun optimize(physicalQueryPlan: PhysicalQueryPlan): PhysicalQueryPlan? {
        return physicalQueryPlan.accept(this)
    }

    override fun visit(projectPlan: ProjectPlan): PhysicalQueryPlan? {
        // Find nested projections
        if (projectPlan.input !is ProjectPlan) return super.visit(projectPlan)
        // Collapse two projections by replacing column references in outer projections with inner projections
        val innerProjections = projectPlan.input.namedColumnExpressions.associate { it.name to it.expr }
        val rewrittenProjections = projectPlan.namedColumnExpressions.map { namedExpression ->
            val rewrittenExpression = ColumnReplacementPass.replaceColumnLiterals(namedExpression.expr) { col ->
                innerProjections[col.columnPath]!!
            }
            NamedExpression(rewrittenExpression, namedExpression.name)
        }
        // Create new projection
        return optimizeOrReturn(PhysicalQueryPlanBuilder.project(projectPlan.input.input, rewrittenProjections))
    }
}