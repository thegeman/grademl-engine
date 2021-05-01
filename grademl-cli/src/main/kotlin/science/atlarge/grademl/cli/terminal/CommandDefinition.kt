package science.atlarge.grademl.cli.terminal

data class CommandDefinition(
    val name: String,
    val usageDescription: String = "",
    val completionDescription: String = usageDescription,
    val options: List<Option> = emptyList(),
    val arguments: List<Argument> = emptyList()
) {

    init {
        // Check the constructor arguments for sanity
        require(name.isNotBlank()) {
            "Command name must not be blank"
        }
        // Check that no two options share a short or long name
        val shortOptions = mutableMapOf<Char, Option>()
        val longOptions = mutableMapOf<String, Option>()
        for (option in options) {
            if (option.shortName != null) {
                require(option.shortName !in shortOptions) {
                    "Option names must be unique, found duplicate short option: \"-${option.shortName}\""
                }
                shortOptions[option.shortName] = option
            }
            if (option.longName != null) {
                require(option.longName !in longOptions) {
                    "Option names must be unique, found duplicate long option: \"--${option.longName}\""
                }
                longOptions[option.longName] = option
            }
        }
        // Check that no two positional arguments share a name
        val argumentNames = mutableSetOf<String>()
        for (argument in arguments) {
            require(argument.name !in argumentNames) {
                "Argument names must be unique, found duplicate argument: \"${argument.name}\""
            }
            argumentNames.add(argument.name)
        }
        // Check that no mandatory arguments are positioned after optional arguments
        val firstOptionalArgumentIndex = arguments.indexOfFirst { it.isOptional }
        val lastMandatoryArgumentIndex = arguments.indexOfLast { !it.isOptional }
        require(firstOptionalArgumentIndex == -1 || firstOptionalArgumentIndex > lastMandatoryArgumentIndex) {
            "CommandParser does not support mandatory arguments placed after optional arguments"
        }
        // Check that only the last argument is variable length (if any)
        require(arguments.dropLast(1).all { !it.isVararg }) {
            "CommandParser does not support arguments placed after a variable-length argument"
        }
    }

}

data class Option(
    val shortName: Char?,
    val longName: String?,
    val description: String,
    val argument: OptionArgument? = null,
    val isOptional: Boolean = true
) {

    init {
        // Check the constructor arguments for sanity
        require(shortName != null || longName != null) {
            "Option requires at least one of short and long name"
        }
        require(shortName == null || shortName.isLetterOrDigit()) {
            "Option's short name must not be a letter or digit"
        }
        require(longName == null || longName.isNotBlank()) {
            "Option's long name must not be blank if specified"
        }
    }

}

data class OptionArgument(
    val display: String,
    val valueConstraint: ArgumentValueConstraint = ArgumentValueConstraint.Any
) {

    init {
        // Check the constructor arguments for sanity
        require(display.isNotBlank()) { "Option argument's display string must not be blank" }
    }

}

data class Argument(
    val name: String,
    val description: String,
    val valueConstraint: ArgumentValueConstraint = ArgumentValueConstraint.Any,
    val isOptional: Boolean = false,
    val isVararg: Boolean = false
) {

    init {
        // Check the constructor arguments for sanity
        require(name.isNotBlank()) { "Argument name must not be blank" }
    }

}

sealed class ArgumentValueConstraint {
    object Any : ArgumentValueConstraint()
    data class Choice(val options: Set<String>) : ArgumentValueConstraint()
    object ExecutionPhasePath : ArgumentValueConstraint()
    object ResourcePath : ArgumentValueConstraint()
    object MetricPath : ArgumentValueConstraint()
}