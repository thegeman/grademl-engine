package science.atlarge.grademl.cli

import science.atlarge.grademl.cli.commands.Command
import science.atlarge.grademl.cli.commands.DisplayExecutionModelCommand
import science.atlarge.grademl.cli.commands.HelpCommand

object CommandRegistry {

    private val commandMap = mutableMapOf<String, Command>()

    val commands
        get() = commandMap.values.toSet()

    operator fun get(commandName: String): Command? = commandMap[commandName]

    fun registerCommand(command: Command) {
        commandMap[command.name] = command
    }

    fun registerCommands(vararg commands: Command) {
        for (command in commands) {
            registerCommand(command)
        }
    }

    init {
        registerCommands(
            DisplayExecutionModelCommand,
            HelpCommand
        )
    }

}

