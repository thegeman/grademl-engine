package science.atlarge.grademl.cli.util

class CommandParser(
    val commandName: String,
    val commandDescription: String = "",
    val options: List<Option> = emptyList(),
    val arguments: List<Argument> = emptyList()
) {

    private val shortOptions: Map<Char, Option>
    private val longOptions: Map<String, Option>

    val usage by lazy {
        // Build usage string
        val sb = StringBuilder()
        // Section 1: Overview of options and arguments
        sb.append("Usage: ").append(commandName)
        // Append options
        for (option in options) {
            sb.append(' ')
            // Add opening bracket/parenthesis
            if (option.isOptional) sb.append('[')
            else if (option.shortName != null && option.longName != null) sb.append('(')
            // Add short option
            if (option.shortName != null) sb.append('-').append(option.shortName)
            // Add OR symbol
            if (option.shortName != null && option.longName != null) sb.append(" | ")
            // Add long option
            if (option.longName != null) sb.append("--").append(option.longName)
            // Add argument name
            if (option.hasArgument) sb.append(' ').append(option.argumentName)
            // Add closing bracket/parenthesis
            if (option.isOptional) sb.append(']')
            else if (option.shortName != null && option.longName != null) sb.append(')')
        }
        // Append arguments
        for (argument in arguments) {
            sb.append(' ')
            // Add argument name if mandatory
            if (!argument.isOptional) {
                sb.append(argument.name)
                if (argument.isVararg) sb.append(' ')
            }
            // Add argument name in brackets if optional or vararg
            if (argument.isOptional || argument.isVararg) {
                sb.append('[')
                sb.append(argument.name)
                // Add ellipses for variable length arguments
                if (argument.isVararg) sb.append(" ...")
                sb.append(']')
            }
        }
        // Section 2: description
        if (commandDescription.isNotBlank()) {
            sb.appendLine()
            sb.append(commandDescription.trimEnd())
        }
        // Section 3: options
        val hasShortOptions = options.any { it.shortName != null }
        val maxLongOptionLength = options.maxOfOrNull { it.longName?.length ?: 0 } ?: 0
        if (options.isNotEmpty()) {
            sb.appendLine()
            sb.appendLine()
            sb.append("Options:")
            for (option in options) {
                sb.appendLine()
                sb.append("  ")
                // Print short name
                if (option.shortName != null) {
                    sb.append('-').append(option.shortName)
                    if (option.longName != null) sb.append(", ")
                } else if (hasShortOptions) {
                    sb.append("    ")
                }
                // Print long name
                if (option.longName != null) {
                    sb.append("--")
                    sb.append(option.longName.padEnd(maxLongOptionLength))
                } else if (maxLongOptionLength > 0) {
                    repeat(maxLongOptionLength + 2) { sb.append(' ') }
                }
                // Print description
                sb.append("  ")
                sb.append(option.description)
            }
        }
        // Section 4: arguments
        if (arguments.isNotEmpty()) {
            sb.appendLine()
            sb.appendLine()
            sb.append("Arguments:")
            val maxArgumentNameLength = arguments.maxOfOrNull { it.name.length } ?: 0
            for (argument in arguments) {
                sb.appendLine()
                sb.append("  ")
                sb.append(argument.name.padEnd(maxArgumentNameLength))
                sb.append("  ")
                sb.append(argument.description)
            }
        }
        // Return the full usage text
        sb.toString()
    }

    init {
        // Check the constructor arguments for sanity
        require(commandName.isNotBlank()) {
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
        this.shortOptions = shortOptions
        this.longOptions = longOptions
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

    fun parse(commandArguments: List<String>): ParseResult {
        // Iterate over "words" to parse options and extract arguments
        val parsedOptions = mutableListOf<Pair<Option, String?>>()
        val argumentsToParse = mutableListOf<String>()
        var index = 0
        while (index < commandArguments.size) {
            val currentArg = commandArguments[index++]
            // Check for a marker closing the option list
            if (currentArg == "--") {
                argumentsToParse.addAll(commandArguments.subList(index, commandArguments.size))
                break
            }
            // Check for a long option
            else if (currentArg.startsWith("--")) {
                // Find the option's specification
                val optionName = currentArg.substring(2)
                val option = longOptions[optionName] ?: return ParseException(
                    "Unrecognized option: \"--$optionName\""
                )
                // Get the option's argument if applicable
                val argument = if (option.hasArgument) {
                    if (index >= commandArguments.size) return ParseException(
                        "Missing mandatory argument to option \"--$optionName\""
                    )
                    commandArguments[index++]
                } else {
                    null
                }
                // Add the parsed option to the list
                parsedOptions.add(option to argument)
            }
            // Check for a short option
            else if (currentArg.startsWith("-")) {
                if (currentArg.length != 2) return ParseException(
                    "Contraction of short options is not supported: \"$currentArg\""
                )
                // Find the option's specification
                val optionName = currentArg[1]
                val option = shortOptions[optionName] ?: return ParseException(
                    "Unrecognized option: \"-$optionName\""
                )
                // Get the option's argument if applicable
                val argument = if (option.hasArgument) {
                    if (index >= commandArguments.size) return ParseException(
                        "Missing mandatory argument to option \"-$optionName\""
                    )
                    commandArguments[index++]
                } else {
                    null
                }
                // Add the parsed option to the list
                parsedOptions.add(option to argument)
            }
            // Otherwise, treat the current argument as a positional argument
            else {
                argumentsToParse.add(currentArg)
            }
        }
        // Parse positional arguments
        val parsedArguments = mutableListOf<Pair<Argument, String>>()
        var givenArgumentIndex = 0
        var expectedArgumentIndex = 0
        while (givenArgumentIndex < argumentsToParse.size && expectedArgumentIndex < arguments.size) {
            val expectedArgument = arguments[expectedArgumentIndex]
            parsedArguments.add(expectedArgument to argumentsToParse[givenArgumentIndex++])
            if (!expectedArgument.isVararg) expectedArgumentIndex++
        }
        // Check if too many arguments were passed
        if (givenArgumentIndex < argumentsToParse.size) {
            return ParseException(
                "Unexpected positional argument encountered: \"${argumentsToParse[givenArgumentIndex]}\""
            )
        }
        // Check if too few arguments were passed
        if (expectedArgumentIndex < arguments.size && !arguments[expectedArgumentIndex].isOptional) {
            return ParseException(
                "Missing mandatory positional argument: \"${arguments[expectedArgumentIndex].name}\""
            )
        }

        return ParsedCommand(parsedOptions, parsedArguments)
    }

}

data class Option(
    val shortName: Char?,
    val longName: String?,
    val description: String,
    val hasArgument: Boolean = false,
    val argumentName: String? = null,
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
        if (hasArgument) {
            require(!argumentName.isNullOrBlank()) { "Option with argument must have an argument name" }
        } else {
            require(argumentName == null) { "Option without argument cannot have an argument name" }
        }
    }

}

data class Argument(
    val name: String,
    val description: String,
    val isOptional: Boolean = false,
    val isVararg: Boolean = false
) {

    init {
        // Check the constructor arguments for sanity
        require(name.isNotBlank()) { "Argument name must not be blank" }
    }

}

sealed class ParseResult

data class ParsedCommand(
    val passedOptions: List<Pair<Option, String?>>,
    val passedArguments: List<Pair<Argument, String>>
) : ParseResult() {

    fun isOptionProvided(option: Option) =
        passedOptions.any { it.first == option }
    fun isOptionProvided(shortName: Char) =
        passedOptions.any { it.first.shortName == shortName }
    fun isOptionProvided(longName: String) =
        passedOptions.any { it.first.longName == longName }

    fun getOptionValue(option: Option) =
        passedOptions.firstOrNull { it.first == option }?.second
    fun getOptionValue(shortName: Char) =
        passedOptions.firstOrNull { it.first.shortName == shortName }?.second
    fun getOptionValue(longName: String) =
        passedOptions.firstOrNull { it.first.longName == longName }?.second

    fun getOptionValues(option: Option) =
        passedOptions.filter { it.first == option }.mapNotNull { it.second }
    fun getOptionValues(shortName: Char) =
        passedOptions.filter { it.first.shortName == shortName }.mapNotNull { it.second }
    fun getOptionValues(longName: String) =
        passedOptions.filter { it.first.longName == longName }.mapNotNull { it.second }

    fun isArgumentProvided(argument: Argument) =
        passedArguments.any { it.first == argument }
    fun isArgumentProvided(name: String) =
        passedArguments.any { it.first.name == name }

    fun getArgumentValue(argument: Argument) =
        passedArguments.firstOrNull { it.first == argument }?.second
    fun getArgumentValue(name: String) =
        passedArguments.firstOrNull { it.first.name == name }?.second

    fun getArgumentValues(argument: Argument) =
        passedArguments.filter { it.first == argument }.map { it.second }
    fun getArgumentValues(name: String) =
        passedArguments.filter { it.first.name == name }.map { it.second }

}

data class ParseException(
    val message: String
) : ParseResult()