package science.atlarge.grademl.query.model

import science.atlarge.grademl.query.language.Type

data class Column(val name: String, val path: String, val index: Int, val type: Type, val isStatic: Boolean) {

    companion object {
        val START_TIME = Column("_start_time", "_start_time", 0, Type.NUMERIC, false)
        val END_TIME = Column("_end_time", "_end_time", 1, Type.NUMERIC, false)
        val DURATION = Column("_duration", "_duration", 2, Type.NUMERIC, false)

        const val INDEX_START_TIME = 0
        const val INDEX_END_TIME = 1
        const val INDEX_DURATION = 2
        const val RESERVED_COLUMNS = 3
    }

}