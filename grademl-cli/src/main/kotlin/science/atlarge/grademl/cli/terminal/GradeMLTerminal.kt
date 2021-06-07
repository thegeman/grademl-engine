package science.atlarge.grademl.cli.terminal

import org.jline.reader.Parser
import org.jline.reader.impl.DefaultParser
import org.jline.reader.impl.history.DefaultHistory
import org.jline.terminal.Terminal
import org.jline.terminal.TerminalBuilder
import science.atlarge.grademl.core.GradeMLJob
import java.nio.file.Path
import kotlin.io.path.readLines

class GradeMLTerminal {

    // Set up the terminal and CLI parsing library
    private val terminal: Terminal = TerminalBuilder.builder()
        .jansi(true)
        .build()
    private val parser = DefaultParser()

    private val interactiveInputSource: TerminalInput by lazy {
        InteractiveInput(terminal, parser)
    }
    private var inputSource: TerminalInput? = null

    fun setInteractiveInput() {
        inputSource = interactiveInputSource
        if (activeJob != null) inputSource!!.setActiveJob(activeJob!!)
    }

    fun setScriptedInput(scriptFile: Path) {
        val scriptLines = scriptFile.readLines()
        inputSource = ScriptedTerminal(scriptLines)
        if (activeJob != null) inputSource!!.setActiveJob(activeJob!!)
    }

    private var activeJob: GradeMLJob? = null
    fun setActiveJob(gradeMLJob: GradeMLJob) {
        activeJob = gradeMLJob
        if (inputSource != null) inputSource!!.setActiveJob(activeJob!!)
    }

    fun setWindowTitle(title: String) {
        if (terminal.type.startsWith("xterm")) {
            terminal.writer().println("\u001B]0;$title\u0007")
        }
    }

    fun readAndParseLine(): List<String>? {
        val line = inputSource!!.readLine() ?: return null
        return parser.parse(line, 0).words()
    }

}

private interface TerminalInput {
    fun setActiveJob(gradeMLJob: GradeMLJob) { }
    fun readLine(): String?
}

private class InteractiveInput(terminal: Terminal, parser: Parser) : TerminalInput {

    // Set up the terminal and CLI parsing library
    private val completer = CommandCompleter()
    private val lineReader = GradeMLLineReader(
        terminal = terminal,
        appName = "GradeML",
        parser = parser,
        completer = completer,
        history = DefaultHistory()
    )

    override fun setActiveJob(gradeMLJob: GradeMLJob) {
        completer.executionModel = gradeMLJob.unifiedExecutionModel
        completer.resourceModel = gradeMLJob.unifiedResourceModel
    }

    override fun readLine(): String {
        while (true) {
            val line = lineReader.readLine("> ")
            if (line.isNotBlank()) {
                return line
            }
        }
    }

}

class ScriptedTerminal(scriptLines: List<String>) : TerminalInput {

    // Filter out blank and commented lines
    private val scriptLines = scriptLines
        .filter { it.isNotBlank() }
        .map { it.trim() }
        .filter { !it.startsWith("#") && !it.startsWith("//") }
    private var nextLineIndex = 0

    override fun readLine(): String? {
        return if (nextLineIndex < scriptLines.size) {
            scriptLines[nextLineIndex++]
        } else {
            null
        }
    }

}