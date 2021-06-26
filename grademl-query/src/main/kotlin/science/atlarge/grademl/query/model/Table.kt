package science.atlarge.grademl.query.model

import science.atlarge.grademl.query.language.Type

interface Table {
    val columns: List<Column>

    fun scan(): RowScanner
}

class Column(val name: String, val path: String, val type: Type)