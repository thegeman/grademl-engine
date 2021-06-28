package science.atlarge.grademl.query.execution

class TypedValue {
    private var _booleanValue = false
    private var _numericValue = 0.0
    private var _stringValue = ""
    private var type = -1

    var booleanValue: Boolean
        get() {
            require(type == 0) { "Value is not of type BOOLEAN" }
            return _booleanValue
        }
        set(value) {
            _booleanValue = value
            type = 0
        }

    var numericValue: Double
        get() {
            require(type == 1) { "Value is not of type NUMERIC" }
            return _numericValue
        }
        set(value) {
            _numericValue = value
            type = 1
        }

    var stringValue: String
        get() {
            require(type == 2) { "Value is not of type STRING" }
            return _stringValue
        }
        set(value) {
            _stringValue = value
            type = 2
        }

}