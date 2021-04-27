package science.atlarge.grademl.cli

import science.atlarge.grademl.cli.commands.Command

object CommandRegistry {

    private val commandMap = mutableMapOf<String, Command>()

    val commands
        get() = commandMap.values.toSet()

    operator fun get(commandName: String): Command? = commandMap[commandName]

    fun registerCommand(command: Command) {
        commandMap[command.name] = command
    }

}

