package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.terminal.Argument
import science.atlarge.grademl.cli.terminal.ArgumentValueConstraint
import science.atlarge.grademl.cli.terminal.ParsedCommand
import science.atlarge.grademl.cli.util.parseResourcePathExpression
import science.atlarge.grademl.cli.util.tryMatchResourcePath

object FilterResourcesCommand : Command(
    name = "filter-resources",
    shortHelpMessage = "exclude or include resources in subsequent commands",
    longHelpMessage = "Adds or removes resources to or from an exclusion list. " +
            "Excluded resources do not appear in the output of most other commands, e.g., plotting.",
    supportedArguments = listOf(
        Argument(
            "exclude|include",
            "exclude or include resources in subsequent commands",
            ArgumentValueConstraint.Choice(setOf("exclude", "include"))
        ),
        Argument(
            "resource_pattern",
            "regular expression matching the path of one or more resources",
            ArgumentValueConstraint.ResourcePath,
            isVararg = true
        )
    )
) {

    override fun process(parsedCommand: ParsedCommand, cliState: CliState) {
        val excludeOrInclude = parsedCommand.getArgumentValue("exclude|include")!!
        if (excludeOrInclude != "exclude" && excludeOrInclude != "include") {
            println("Invalid argument: \"$excludeOrInclude\". Expected \"exclude\" or \"include\".")
            return
        }

        val pathExpressions = parsedCommand.getArgumentValues("resource_pattern")
        val paths = pathExpressions.map(::parseResourcePathExpression)
        val matchedResources = paths.flatMap { tryMatchResourcePath(it, cliState) ?: return }.toSet()

        val previousSelectionSize = cliState.selectedResources.size
        if (excludeOrInclude == "exclude") cliState.excludeResources(matchedResources)
        else cliState.includeResources(matchedResources)
        val newSelectionSize = cliState.selectedResources.size

        if (excludeOrInclude == "exclude") {
            println("Excluded ${previousSelectionSize - newSelectionSize} previously included resource(s).")
        } else {
            println("Included ${newSelectionSize - previousSelectionSize} previously excluded resource(s).")
        }
    }

}