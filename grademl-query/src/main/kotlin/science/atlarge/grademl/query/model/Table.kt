package science.atlarge.grademl.query.model

class Table(val columns: List<Column>)

class Column(val name: String, val path: String)