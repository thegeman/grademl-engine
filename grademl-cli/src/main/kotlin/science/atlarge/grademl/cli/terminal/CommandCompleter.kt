package science.atlarge.grademl.cli.terminal

import org.jline.reader.Candidate
import org.jline.reader.Completer
import org.jline.reader.LineReader
import org.jline.reader.ParsedLine
import science.atlarge.grademl.cli.CliState
import science.atlarge.grademl.cli.CommandRegistry
import science.atlarge.grademl.core.Path
import science.atlarge.grademl.core.PathMatches

class CommandCompleter(
    private val cliState: CliState
) : Completer {

    override fun complete(reader: LineReader, line: ParsedLine, candidates: MutableList<Candidate>) {
        if (line.wordIndex() == 0) {
            completeCommand(candidates)
        } else {
            completeArgumentsForCommand(line.words()[0], line, candidates)
        }
    }

    private fun completeCommand(candidates: MutableList<Candidate>) {
        for (command in CommandRegistry.commands) {
            candidates.add(
                Candidate(
                    command.name, command.name, null, command.definition.shortDescription,
                    null, null, true
                )
            )
        }
    }

    private fun completeArgumentsForCommand(
        command: String,
        line: ParsedLine,
        candidates: MutableList<Candidate>
    ) {
        // Use CommandParser to determine which CommandTokens can be passed at the cursor
        val previousWords = line.words().subList(1, line.wordIndex())
        val commandParser = CommandParser.forCommand(command) ?: return
        val acceptedTokens = commandParser.nextAcceptedTokens(previousWords)
        // Convert tokens to completion candidates
        if (line.wordCursor() > 0 && line.word().startsWith("-")) {
            for (option in acceptedTokens.acceptedOptions) {
                addOptionCandidates(
                    line.word().substring(0, line.wordCursor()),
                    option,
                    candidates
                )
            }
        } else if (acceptedTokens.acceptedArgumentType != null) {
            addArgumentCandidates(
                line.word().substring(0, line.wordCursor()),
                acceptedTokens.acceptedArgumentType,
                candidates
            )
        }
    }

    private fun addOptionCandidates(partialWord: String, option: Option, candidates: MutableList<Candidate>) {
        val shouldAddLongOption = option.longName != null && !(partialWord.length >= 2 && partialWord[1] != '-')
        if (shouldAddLongOption) {
            candidates.add(
                Candidate(
                    "--${option.longName}",
                    "--${option.longName}",
                    null,
                    option.description,
                    null,
                    null,
                    true
                )
            )
        } else {
            candidates.add(
                Candidate(
                    "-${option.shortName}",
                    "-${option.shortName}",
                    null,
                    option.description,
                    null,
                    null,
                    true
                )
            )
        }
    }

    private fun addArgumentCandidates(
        partialWord: String,
        argument: ArgumentValueConstraint,
        candidates: MutableList<Candidate>
    ) {
        when (argument) {
            ArgumentValueConstraint.Any -> return
            is ArgumentValueConstraint.Choice -> {
                argument.options.mapTo(candidates) { opt ->
                    Candidate(opt, opt, null, null, null, null, true)
                }
            }
            ArgumentValueConstraint.ExecutionPhasePath -> addExecutionPathCandidates(partialWord, candidates)
            ArgumentValueConstraint.ResourcePath -> addResourcePathCandidates(partialWord, candidates)
            ArgumentValueConstraint.MetricPath -> addMetricPathCandidates(partialWord, candidates)
        }
    }

    private fun addExecutionPathCandidates(pathExpression: String, candidates: MutableList<Candidate>) {
        // If the current expression is empty, suggest children of the root phase
        if (pathExpression.isEmpty()) {
            for (phase in cliState.executionModel.rootPhase.children) {
                candidates.add(
                    Candidate(
                        "${Path.SEPARATOR}${phase.name}",
                        "${Path.SEPARATOR}${phase.name}",
                        null,
                        null,
                        null,
                        null,
                        false
                    )
                )
            }
            return
        }
        // Parse path
        val path = Path.parse(pathExpression)
        val pathWithoutLastComponent = Path(path.pathComponents.dropLast(1), path.isRelative)
        val lastComponent = path.pathComponents.lastOrNull() ?: ""
        // Resolve "locked in" part of the path
        val phaseMatchResult = cliState.executionModel.resolvePath(pathWithoutLastComponent)
        if (phaseMatchResult !is PathMatches) return
        val matchedPhases = phaseMatchResult.matches
        // Check if the next path component start with a (double) dot
        if (lastComponent.startsWith(".")) {
            if (matchedPhases.any { !it.isRoot }) {
                candidates.add(
                    Candidate(
                        "..",
                        "..",
                        null,
                        null,
                        "/",
                        null,
                        false
                    )
                )
            }
            return
        }
        // Otherwise, find all possible subphases
        val possiblePhases = matchedPhases.flatMap { it.children }
        val possiblePaths = if ('[' in lastComponent) {
            possiblePhases.map { it.identifier }
        } else {
            possiblePhases.map { it.name }
        }.toSet()
        // Create candidates
        for (possiblePath in possiblePaths) {
            candidates.add(
                Candidate(
                    pathWithoutLastComponent.resolve(possiblePath).toString(),
                    possiblePath,
                    null,
                    null,
                    null,
                    null,
                    false
                )
            )
        }
    }

    private fun addResourcePathCandidates(
        pathExpression: String,
        candidates: MutableList<Candidate>,
        filterResourcesWithMetrics: Boolean = false
    ) {
        // If the current expression is empty, suggest children of the root resource
        if (pathExpression.isEmpty()) {
            val rootResources = cliState.resourceModel.rootResource.children
            val filteredResources = if (filterResourcesWithMetrics) {
                rootResources.filter { it.metricsInTree.isNotEmpty() }
            } else {
                rootResources
            }
            for (resource in filteredResources.map { it.name }.toSet()) {
                candidates.add(
                    Candidate(
                        "${Path.SEPARATOR}${resource}",
                        "${Path.SEPARATOR}${resource}",
                        null,
                        null,
                        null,
                        null,
                        false
                    )
                )
            }
            return
        }
        // Parse path
        val path = Path.parse(pathExpression)
        val pathWithoutLastComponent = if (pathExpression.endsWith(Path.SEPARATOR)) {
            path
        } else {
            Path(path.pathComponents.dropLast(1), path.isRelative)
        }
        val lastComponent = if (pathExpression.endsWith(Path.SEPARATOR)) {
            ""
        } else {
            path.pathComponents.last()
        }
        // Resolve "locked in" part of the path
        val resourceMatchResult = cliState.resourceModel.resolvePath(pathWithoutLastComponent)
        if (resourceMatchResult !is PathMatches) return
        val matchedResources = resourceMatchResult.matches
        // Check if the next path component start with a (double) dot
        if (lastComponent.startsWith(".")) {
            if (matchedResources.any { !it.isRoot }) {
                candidates.add(
                    Candidate(
                        "..",
                        "..",
                        null,
                        null,
                        null,
                        null,
                        false
                    )
                )
            }
            return
        }
        // Otherwise, find all possible subresources
        val possibleResources = matchedResources.flatMap { it.children }.let { resources ->
            if (filterResourcesWithMetrics) {
                resources.filter { it.metricsInTree.isNotEmpty() }
            } else {
                resources
            }
        }
        val possiblePaths = if ('[' in lastComponent) {
            possibleResources.map { it.identifier }
        } else {
            possibleResources.map { it.name }
        }.toSet()
        // Create candidates
        for (possiblePath in possiblePaths) {
            candidates.add(
                Candidate(
                    pathWithoutLastComponent.resolve(possiblePath).toString(),
                    possiblePath,
                    null,
                    null,
                    null,
                    null,
                    false
                )
            )
        }
    }

    private fun addMetricPathCandidates(pathExpression: String, candidates: MutableList<Candidate>) {
        // If the current expression is empty, suggest a resource path first
        if (pathExpression.isEmpty()) {
            addResourcePathCandidates(pathExpression, candidates, true)
            return
        }
        // Split on metric path separator and suggest metric name candidates if applicable
        if (':' in pathExpression) {
            val resourcePathStr = pathExpression.split(':').first()
            val resourcePath = Path.parse(resourcePathStr)
            val resourcePathMatches = cliState.resourceModel.resolvePath(resourcePath) as? PathMatches ?: return
            val validMetrics = resourcePathMatches.matches.flatMap { it.metrics }.map { it.name }.toSet()
            for (validMetric in validMetrics) {
                candidates.add(
                    Candidate(
                        "$resourcePathStr:$validMetric",
                        validMetric,
                        null,
                        null,
                        null,
                        null,
                        true
                    )
                )
            }
            return
        }
        // If current path maps to one or more resources, suggest metrics
        if (!pathExpression.endsWith(Path.SEPARATOR)) {
            val pathMatches = cliState.resourceModel.resolvePath(Path.parse(pathExpression)) as? PathMatches
            if (pathMatches != null && pathMatches.matches.any { it.metrics.isNotEmpty() }) {
                for (metric in pathMatches.matches.flatMap { it.metrics }.map { it.name }.toSet()) {
                    candidates.add(
                        Candidate(
                            "$pathExpression:$metric",
                            ":$metric",
                            null,
                            null,
                            null,
                            null,
                            true
                        )
                    )
                }
                // If there are any subresources with metrics, also suggest a / to continue the resource path
                if (pathMatches.matches.any { it.children.any { child -> child.metricsInTree.isNotEmpty() } }) {
                    candidates.add(
                        Candidate(
                            "$pathExpression${Path.SEPARATOR}",
                            Path.SEPARATOR.toString(),
                            null,
                            null,
                            null,
                            null,
                            false
                        )
                    )
                }
                return
            }
        }
        // Otherwise, suggest resource paths
        addResourcePathCandidates(pathExpression, candidates, true)
    }

}