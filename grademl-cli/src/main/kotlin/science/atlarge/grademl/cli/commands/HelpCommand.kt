package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.CommandRegistry
import science.atlarge.grademl.cli.util.Argument
import science.atlarge.grademl.cli.util.ParsedCommand

object HelpCommand : Command(
    name = "help",
    shortHelpMessage = "display this message",
    longHelpMessage = "Use \"help\" for a list of available commands, " +
            "or \"help [command]\" for detailed information about a particular command.",
    supportedArguments = listOf(
        Argument(
            name = "command",
            isOptional = true,
            description = "name of command to display detailed information for"
        )
    )
) {

    override fun process(parsedCommand: ParsedCommand, cliState: CliState) {
        val commandArg = parsedCommand.getArgumentValue("command")
        if (commandArg == null) displayShortHelp()
        else displayLongHelp(commandArg)
    }

    private fun displayShortHelp() {
        println("The following commands are available:")

        val sortedCommands = CommandRegistry.commands.sortedBy(Command::name)
        val maxCommandLength = sortedCommands.map { it.name.length }.maxOrNull() ?: 0
        for (commandProcessor in sortedCommands) {
            println("    ${commandProcessor.name.padEnd(maxCommandLength)}    ${commandProcessor.shortHelpMessage}")
        }

        println("Use \"help [command]\" to get additional information about a command.")
    }

    private fun displayLongHelp(commandName: String) {
        val command = CommandRegistry[commandName]
        if (command == null) {
            println("Cannot display help message for unknown command: \"$commandName\".")
            return
        }

        println(command.longHelpMessage)
    }

}