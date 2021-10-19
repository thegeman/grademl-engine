package science.atlarge.grademl.query.plan.physical

interface PhysicalQueryPlanVisitor<out T> {

    fun visit(filterPlan: FilterPlan): T
    fun visit(linearTableScanPlan: LinearTableScanPlan): T
    fun visit(projectPlan: ProjectPlan): T
    fun visit(sortedTemporalJoinPlan: SortedTemporalJoinPlan): T
    fun visit(sortPlan: SortPlan): T

}