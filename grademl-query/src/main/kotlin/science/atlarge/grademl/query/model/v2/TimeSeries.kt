package science.atlarge.grademl.query.model.v2

interface TimeSeries : Row {

    fun rowIterator(): RowIterator

}