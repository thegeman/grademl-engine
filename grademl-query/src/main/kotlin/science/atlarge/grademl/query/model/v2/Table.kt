package science.atlarge.grademl.query.model.v2

interface Table {

    val schema: TableSchema

    fun timeSeriesIterator(): TimeSeriesIterator

}