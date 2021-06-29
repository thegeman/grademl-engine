package science.atlarge.grademl.query.model

import science.atlarge.grademl.query.ensureExhaustive
import science.atlarge.grademl.query.language.Type.*

class TypedValue {
    private var _booleanValue = false
    private var _numericValue = 0.0
    private var _stringValue = ""
    private var type = UNDEFINED

    val isUndefined: Boolean
        get() = type == UNDEFINED

    val isBoolean: Boolean
        get() = type == BOOLEAN
    var booleanValue: Boolean
        get() {
            require(type == BOOLEAN) { "Value is not of type BOOLEAN" }
            return _booleanValue
        }
        set(value) {
            _booleanValue = value
            type = BOOLEAN
        }

    val isNumeric: Boolean
        get() = type == NUMERIC
    var numericValue: Double
        get() {
            require(type == NUMERIC) { "Value is not of type NUMERIC" }
            return _numericValue
        }
        set(value) {
            _numericValue = value
            type = NUMERIC
        }

    val isString: Boolean
        get() = type == STRING
    var stringValue: String
        get() {
            require(type == STRING) { "Value is not of type STRING" }
            return _stringValue
        }
        set(value) {
            _stringValue = value
            type = STRING
        }

    fun clear() {
        type = UNDEFINED
    }

    fun clone(): TypedValue {
        return TypedValue().also { copyTo(it) }
    }

    fun copyTo(other: TypedValue) {
        when (type) {
            UNDEFINED -> other.clear()
            BOOLEAN -> other.booleanValue = _booleanValue
            NUMERIC -> other.numericValue = _numericValue
            STRING -> other.stringValue = _stringValue
        }.ensureExhaustive
    }

    fun copyFrom(other: TypedValue) {
        other.copyTo(this)
    }

}