package science.atlarge.grademl.cli.commands

interface Command {

    val name: String
    val shortHelpMessage: String
    val longHelpMessage: String

    fun process(arguments: List<String>)

}