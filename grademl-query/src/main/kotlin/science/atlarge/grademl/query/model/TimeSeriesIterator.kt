package science.atlarge.grademl.query.model

interface TimeSeriesIterator {

    val schema: TableSchema

    val currentTimeSeries: TimeSeries

    fun loadNext(): Boolean

}