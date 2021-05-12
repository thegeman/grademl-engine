package science.atlarge.grademl.cli.terminal

import org.jline.reader.*
import org.jline.reader.impl.LineReaderImpl
import org.jline.terminal.Terminal
import org.jline.terminal.TerminalBuilder
import java.util.*

class GradeMLLineReader(
    terminal: Terminal? = null,
    appName: String = "",
    variables: Map<String, String> = emptyMap(),
    options: Map<LineReader.Option, Boolean> = emptyMap(),
    history: History? = null,
    completer: Completer? = null,
    highlighter: Highlighter? = null,
    parser: Parser? = null,
    expander: Expander? = null,
    completionMatcher: CompletionMatcher? = null
) : LineReaderImpl(terminal ?: TerminalBuilder.terminal(), appName, variables) {

    init {
        options.forEach(this::option)
        if (history != null) this.history = history
        if (completer != null) this.completer = completer
        if (highlighter != null) this.highlighter = highlighter
        if (parser != null) this.parser = parser
        if (expander != null) this.expander = expander
        if (completionMatcher != null) this.completionMatcher = completionMatcher

        widgets[BEGIN_PASTE] = createPasteWidget()
    }

    private fun createPasteWidget() = object : Widget {
        private val pastedLineBuffer: Queue<String> = LinkedList()

        // Override paste behavior to always interpret newline characters as command confirmation
        override fun apply(): Boolean {
            if (pastedLineBuffer.isNotEmpty()) {
                // If there are any buffered lines from a previous paste action, process the next line from the buffer
                val line = pastedLineBuffer.poll()
                buffer.write(line)
            } else {
                val str = doReadStringUntil(BRACKETED_PASTE_END).replace('\r', '\n')
                if ('\n' in str) {
                    // Process the first line
                    val (firstLine, remainingStr) = str.split('\n', limit = 2)
                    buffer.write(firstLine)
                    // Split the remaining lines into individually pasted lines and buffer them
                    remainingStr.split("\n").forEach(pastedLineBuffer::offer)
                } else {
                    buffer.write(str)
                }
            }
            // If there are more lines to paste, confirm the current line with a newline and begin pasting the next one
            if (pastedLineBuffer.isNotEmpty()) {
                runMacro("\n$BRACKETED_PASTE_BEGIN")
            }
            return true
        }
    }

}