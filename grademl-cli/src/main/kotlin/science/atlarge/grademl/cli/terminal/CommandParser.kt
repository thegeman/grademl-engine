package science.atlarge.grademl.cli.terminal

class CommandParser(
    val commandDefinition: CommandDefinition
) {

    private val shortOptions: Map<Char, Option> = commandDefinition.options
        .filter { it.shortName != null }
        .associateBy { it.shortName!! }
    private val longOptions: Map<String, Option> = commandDefinition.options
        .filter { it.longName != null }
        .associateBy { it.longName!! }

    val usage by lazy {
        // Build usage string
        val sb = StringBuilder()
        // Section 1: Overview of options and arguments
        sb.append("Usage: ").append(commandDefinition.name)
        // Append options
        for (option in commandDefinition.options) {
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
            if (option.argument != null) sb.append(' ').append(option.argument.display)
            // Add closing bracket/parenthesis
            if (option.isOptional) sb.append(']')
            else if (option.shortName != null && option.longName != null) sb.append(')')
        }
        // Append arguments
        for (argument in commandDefinition.arguments) {
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
        if (commandDefinition.usageDescription.isNotBlank()) {
            sb.appendLine()
            sb.append(commandDefinition.usageDescription.trimEnd())
        }
        // Section 3: options
        val hasShortOptions = shortOptions.isNotEmpty()
        val maxLongOptionLength = longOptions.maxOfOrNull { it.key.length } ?: 0
        if (commandDefinition.options.isNotEmpty()) {
            sb.appendLine()
            sb.appendLine()
            sb.append("Options:")
            for (option in commandDefinition.options) {
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
                sb.append("    ")
                sb.append(option.description)
            }
        }
        // Section 4: arguments
        if (commandDefinition.arguments.isNotEmpty()) {
            sb.appendLine()
            sb.appendLine()
            sb.append("Arguments:")
            val maxArgumentNameLength = commandDefinition.arguments.maxOfOrNull { it.name.length } ?: 0
            for (argument in commandDefinition.arguments) {
                sb.appendLine()
                sb.append("  ")
                sb.append(argument.name.padEnd(maxArgumentNameLength))
                sb.append("    ")
                sb.append(argument.description)
            }
        }
        // Return the full usage text
        sb.toString()
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
                val argument = if (option.argument != null) {
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
                val argument = if (option.argument != null) {
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
        while (givenArgumentIndex < argumentsToParse.size && expectedArgumentIndex < commandDefinition.arguments.size) {
            val expectedArgument = commandDefinition.arguments[expectedArgumentIndex]
            parsedArguments.add(expectedArgument to argumentsToParse[givenArgumentIndex++])
            if (!expectedArgument.isVararg) expectedArgumentIndex++
        }
        // Check if too many arguments were passed
        if (givenArgumentIndex < argumentsToParse.size) {
            return ParseException(
                "Unexpected positional argument encountered: \"${argumentsToParse[givenArgumentIndex]}\""
            )
        }
        // Check if too few arguments were passed:
        // - more arguments could have been parsed
        // - the next argument is not optional
        // - the next argument is not vararg OR has not been encountered yet
        if (expectedArgumentIndex < commandDefinition.arguments.size) {
            val nextArg = commandDefinition.arguments[expectedArgumentIndex]
            val lastArgMatchesNextArg = parsedArguments.lastOrNull()?.first == nextArg
            if (!nextArg.isOptional && (!nextArg.isVararg || !lastArgMatchesNextArg)) {
                return ParseException(
                    "Missing mandatory positional argument: " +
                            "\"${commandDefinition.arguments[expectedArgumentIndex].name}\""
                )
            }
        }

        return ParsedCommand(parsedOptions, parsedArguments)
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