package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState

interface Command {

    val name: String
    val shortHelpMessage: String
    val longHelpMessage: String

    fun process(arguments: List<String>, cliState: CliState)

}