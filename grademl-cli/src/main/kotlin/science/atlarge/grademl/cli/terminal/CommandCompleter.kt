package science.atlarge.grademl.cli.terminal

import org.jline.reader.Candidate
import org.jline.reader.Completer
import org.jline.reader.LineReader
import org.jline.reader.ParsedLine
import science.atlarge.grademl.cli.CommandRegistry

object CommandCompleter : Completer {

    override fun complete(reader: LineReader, line: ParsedLine, candidates: MutableList<Candidate>) {
        if (line.wordIndex() == 0) {
            completeCommand(candidates)
        } else {
            completeArgumentsForCommand(line.words()[0], reader, line, candidates)
        }
    }

    private fun completeCommand(candidates: MutableList<Candidate>) {
        for (command in CommandRegistry.commands) {
            candidates.add(
                Candidate(
                    command.name, command.name, null, command.shortHelpMessage,
                    null, null, true
                )
            )
        }
    }

    private fun completeArgumentsForCommand(
        command: String,
        reader: LineReader,
        line: ParsedLine,
        candidates: MutableList<Candidate>
    ) {
        // TODO: Complete options and arguments with limited options such as paths
    }

}