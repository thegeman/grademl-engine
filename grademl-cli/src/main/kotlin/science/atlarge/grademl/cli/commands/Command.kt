package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.terminal.*

abstract class Command(
    name: String,
    shortHelpMessage: String,
    longHelpMessage: String,
    supportedOptions: List<Option> = emptyList(),
    supportedArguments: List<Argument> = emptyList()
) {

    val definition = CommandDefinition(
        name,
        shortHelpMessage,
        longHelpMessage,
        supportedOptions,
        supportedArguments
    )

    private val commandParser = CommandParser(definition)

    val name: String
        get() = definition.name

    fun process(arguments: List<String>, cliState: CliState) {
        val parseResult = commandParser.parse(arguments)
        if (parseResult is ParseException) {
            println("Encountered an error while parsing the command:")
            println("  ${parseResult.message}")
            return
        }
        val parsedCommand = parseResult as ParsedCommand
        process(parsedCommand, cliState)
    }

    protected abstract fun process(parsedCommand: ParsedCommand, cliState: CliState)

}