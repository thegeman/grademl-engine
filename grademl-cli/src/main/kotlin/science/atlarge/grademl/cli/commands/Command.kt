package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.util.*

abstract class Command(
    val name: String,
    val shortHelpMessage: String,
    longHelpMessage: String,
    supportedOptions: List<Option> = emptyList(),
    supportedArguments: List<Argument> = emptyList()
) {

    private val commandParser = CommandParser(name, longHelpMessage, supportedOptions, supportedArguments)

    val longHelpMessage: String
        get() = commandParser.usage

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