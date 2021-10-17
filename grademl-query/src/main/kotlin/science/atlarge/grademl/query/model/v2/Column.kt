package science.atlarge.grademl.query.model.v2

import science.atlarge.grademl.query.language.Type

data class Column(val identifier: String, val type: Type, val isKey: Boolean)