package science.atlarge.grademl.query.model.v2

interface TimeSeriesIterator {

    val schema: TableSchema

    val currentTimeSeries: TimeSeries

    fun loadNext(): Boolean

}