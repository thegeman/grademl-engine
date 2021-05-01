package science.atlarge.grademl.cli.terminal

class CommandParser(
    private val commandDefinition: CommandDefinition
) {

    private val shortOptions: Map<Char, Option> = commandDefinition.options
        .filter { it.shortName != null }
        .associateBy { it.shortName!! }
    private val longOptions: Map<String, Option> = commandDefinition.options
        .filter { it.longName != null }
        .associateBy { it.longName!! }

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