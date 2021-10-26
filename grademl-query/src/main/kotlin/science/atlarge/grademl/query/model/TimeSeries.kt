package science.atlarge.grademl.query.model

interface TimeSeries : Row {

    fun rowIterator(): RowIterator

}