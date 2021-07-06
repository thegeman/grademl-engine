package science.atlarge.grademl.query.model

import science.atlarge.grademl.query.ensureExhaustive
import science.atlarge.grademl.query.language.Type.*

class TypedValue() : Comparable<TypedValue> {
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

    constructor(booleanValue: Boolean) : this() {
        this.booleanValue = booleanValue
    }

    constructor(numericValue: Double) : this() {
        this.numericValue = numericValue
    }

    constructor(stringValue: String) : this() {
        this.stringValue = stringValue
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

    override fun compareTo(other: TypedValue): Int {
        if (type != other.type) throw UnsupportedOperationException()
        return when (type) {
            UNDEFINED -> 0
            BOOLEAN -> _booleanValue.compareTo(other._booleanValue)
            NUMERIC -> _numericValue.compareTo(other._numericValue)
            STRING -> _stringValue.compareTo(other._stringValue)
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as TypedValue

        if (type != other.type) return false
        return when (type) {
            UNDEFINED -> true
            BOOLEAN -> _booleanValue == other._booleanValue
            NUMERIC -> _numericValue == other._numericValue
            STRING -> _stringValue == other._stringValue
        }
    }

    override fun hashCode(): Int {
        return type.ordinal + 13 * when (type) {
            UNDEFINED -> 0
            BOOLEAN -> if (_booleanValue) 1 else 0
            NUMERIC -> _numericValue.hashCode()
            STRING -> _stringValue.hashCode()
        }
    }

    override fun toString(): String {
        return when (type) {
            UNDEFINED -> "UNDEFINED"
            BOOLEAN -> if (_booleanValue) "TRUE" else "FALSE"
            NUMERIC -> _numericValue.toString()
            STRING -> "\"$_stringValue\""
        }
    }

}