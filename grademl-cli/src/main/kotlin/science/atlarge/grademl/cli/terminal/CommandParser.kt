package science.atlarge.grademl.cli.terminal

import science.atlarge.grademl.cli.CommandRegistry

class CommandParser(
    private val commandDefinition: CommandDefinition
) {

    private val shortOptions: Map<Char, Option> = commandDefinition.options
        .filter { it.shortName != null }
        .associateBy { it.shortName!! }
    private val longOptions: Map<String, Option> = commandDefinition.options
        .filter { it.longName != null }
        .associateBy { it.longName!! }

    fun nextAcceptedTokens(commandArguments: List<String>): AcceptedCommandTokens {
        // Tokenize the command arguments
        val tokens = CommandTokenizer.tokenizeCommand(commandArguments)
        // Check if a specific option's argument is expected
        val argument = tokens.lastOrNull()?.let(this::lookupOption)?.argument
        if (argument != null) {
            return AcceptedCommandTokens(emptySet(), argument.valueConstraint)
        }
        // Otherwise check if options are still allowed
        val acceptedOptions = if (tokens.any { it is EndOfOptionsToken }) {
            emptySet()
        } else {
            commandDefinition.options.toSet()
        }
        // Check which positional argument is next, if any
        var positionalArgumentsGiven = 0
        var eatNextArgument = false
        for (token in tokens) {
            if (token is ArgumentToken && !eatNextArgument) positionalArgumentsGiven++
            eatNextArgument = lookupOption(token)?.argument != null
        }
        val nextArgument = when {
            positionalArgumentsGiven < commandDefinition.arguments.lastIndex -> {
                commandDefinition.arguments[positionalArgumentsGiven]
            }
            commandDefinition.arguments.lastOrNull()?.isVararg == true -> {
                commandDefinition.arguments.last()
            }
            else -> null
        }
        return AcceptedCommandTokens(acceptedOptions, nextArgument?.valueConstraint)
    }

    private fun lookupOption(token: CommandToken): Option? {
        if (token is ShortOptionToken) return shortOptions[token.option]
        if (token is LongOptionToken) return longOptions[token.option]
        return null
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

    companion object {

        private val parsers = mutableMapOf<String, CommandParser>()

        fun forCommand(commandName: String): CommandParser? {
            if (commandName !in parsers) {
                val command = CommandRegistry[commandName] ?: return null
                parsers[commandName] = CommandParser(command.definition)
            }
            return parsers[commandName]
        }

    }

}

data class AcceptedCommandTokens(
    val acceptedOptions: Set<Option> = emptySet(),
    val acceptedArgumentType: ArgumentValueConstraint? = null
)

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