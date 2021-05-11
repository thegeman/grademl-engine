package science.atlarge.grademl.cli.commands

import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.terminal.Argument
import science.atlarge.grademl.cli.terminal.ArgumentValueConstraint
import science.atlarge.grademl.cli.terminal.ParsedCommand
import science.atlarge.grademl.cli.util.parseExecutionPhasePathExpression
import science.atlarge.grademl.cli.util.tryMatchExecutionPhasePath

object FilterPhasesCommand : Command(
    name = "filter-phases",
    shortHelpMessage = "exclude or include phases in subsequent commands",
    longHelpMessage = "Adds or removes phases to or from an exclusion list. " +
            "Excluded phases do not appear in the output of most other commands, e.g., plotting.",
    supportedArguments = listOf(
        Argument(
            "exclude|include",
            "exclude or include phases in subsequent commands",
            ArgumentValueConstraint.Choice(setOf("exclude", "include"))
        ),
        Argument(
            "phase_pattern",
            "regular expression matching the path of one or more phases",
            ArgumentValueConstraint.ExecutionPhasePath,
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

        val pathExpressions = parsedCommand.getArgumentValues("phase_pattern")
        val paths = pathExpressions.map(::parseExecutionPhasePathExpression)
        val matchedPhases = paths.flatMap {
            tryMatchExecutionPhasePath(it, cliState, restrictToSelected = false) ?: return
        }.toSet()

        val previousSelectionSize = cliState.selectedPhases.size
        if (excludeOrInclude == "exclude") cliState.excludePhases(matchedPhases)
        else cliState.includePhases(matchedPhases)
        val newSelectionSize = cliState.selectedPhases.size

        if (excludeOrInclude == "exclude") {
            println("Excluded ${previousSelectionSize - newSelectionSize} previously included phase(s).")
        } else {
            println("Included ${newSelectionSize - previousSelectionSize} previously excluded phase(s).")
        }
    }

}