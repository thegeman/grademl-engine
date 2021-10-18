package science.atlarge.grademl.query.execution.operators

import science.atlarge.grademl.query.model.v2.TableSchema
import science.atlarge.grademl.query.model.v2.TimeSeriesIterator

interface QueryOperator {

    val schema: TableSchema

    fun execute(): TimeSeriesIterator

}