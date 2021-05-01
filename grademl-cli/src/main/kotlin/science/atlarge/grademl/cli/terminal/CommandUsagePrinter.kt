package science.atlarge.grademl.cli.terminal

import science.atlarge.grademl.cli.CommandRegistry

object CommandUsagePrinter {

    private val cachedUsageForCommands = mutableMapOf<String, String>()

    fun usageForCommand(commandName: String): String? {
        if (commandName !in cachedUsageForCommands) {
            val commandDefinition = CommandRegistry[commandName]?.definition ?: return null
            cachedUsageForCommands[commandName] = generateUsageString(commandDefinition)
        }
        return cachedUsageForCommands[commandName]
    }

    private fun generateUsageString(commandDefinition: CommandDefinition): String {
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
        if (commandDefinition.longDescription.isNotBlank()) {
            sb.appendLine()
            sb.append(commandDefinition.longDescription.trimEnd())
        }
        // Section 3: options
        val hasShortOptions = commandDefinition.options.any { it.shortName != null }
        val maxLongOptionLength = commandDefinition.options.mapNotNull { it.longName }.maxOfOrNull { it.length } ?: 0
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
        return sb.toString()
    }

}