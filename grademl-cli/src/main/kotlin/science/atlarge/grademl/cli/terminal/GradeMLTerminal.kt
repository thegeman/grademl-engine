package science.atlarge.grademl.cli.terminal

import org.jline.reader.impl.DefaultParser
import org.jline.reader.impl.history.DefaultHistory
import org.jline.terminal.TerminalBuilder
import science.atlarge.grademl.core.GradeMLJob

class GradeMLTerminal {

    // Set up the terminal and CLI parsing library
    private val terminal = TerminalBuilder.builder()
        .jansi(true)
        .build()
    private val parser = DefaultParser()
    private val completer = CommandCompleter()
    private val lineReader = GradeMLLineReader(
        terminal = terminal,
        appName = "GradeML",
        parser = parser,
        completer = completer,
        history = DefaultHistory()
    )

    private lateinit var activeJob: GradeMLJob
    fun setActiveJob(gradeMLJob: GradeMLJob) {
        activeJob = gradeMLJob
        completer.executionModel = gradeMLJob.unifiedExecutionModel
        completer.resourceModel = gradeMLJob.unifiedResourceModel
    }

    fun setWindowTitle(title: String) {
        if (terminal.type.startsWith("xterm")) {
            terminal.writer().println("\u001B]0;$title\u0007")
        }
    }

    fun readAndParseLine(): List<String> {
        while (true) {
            val line = lineReader.readLine("> ")
            if (line.isNotBlank()) {
                return parser.parse(line, 0).words()
            }
        }
    }

}