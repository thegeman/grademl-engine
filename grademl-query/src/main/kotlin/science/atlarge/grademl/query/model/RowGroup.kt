package science.atlarge.grademl.query.model

abstract class RowGroup : RowScanner() {

    abstract val columns: List<Column>

    abstract val groupedColumnIndices: List<Int>

    abstract fun readGroupColumnValue(columnId: Int, outValue: TypedValue): TypedValue

}