package science.atlarge.grademl.query.model

import science.atlarge.grademl.query.language.Type

data class Column(val name: String, val path: String, val type: Type, val function: ColumnFunction)