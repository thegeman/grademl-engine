package science.atlarge.grademl.input.spark

import jdk.jfr.consumer.RecordingFile
import java.lang.StringBuilder
import java.nio.file.Files
import java.nio.file.Path
import kotlin.math.exp
import kotlin.streams.toList

class StackTrace {
    private val rootNode = Node("<root>")

    val children: Map<String, Node>
        get() = rootNode.children

    val traceCount: Int
        get() = rootNode.traceCount

    fun addTrace(trace: List<String>) {
        require(trace.isNotEmpty())
        rootNode.addTrace(trace, 0)
    }

    fun dump(expandUntil: Int = 0): String {
        val sb = StringBuilder()
        rootNode.appendTo(sb, 0, expandUntil)
        return sb.toString()
    }

    class Node(val method: String) {
        private val _children = mutableMapOf<String, Node>()
        val children: Map<String, Node>
            get() = _children

        var traceCount = 0
            private set
        var selfCount = 0
            private set

        internal fun addTrace(trace: List<String>, nextOffset: Int) {
            traceCount++
            if (nextOffset <= trace.lastIndex) {
                val nextMethod = trace[nextOffset]
                _children.getOrPut(nextMethod) { Node(nextMethod) }.addTrace(trace, nextOffset + 1)
            } else {
                selfCount++
            }
        }

        internal fun appendTo(sb: StringBuilder, level: Int, expandUntil: Int) {
            repeat(level) { sb.append("  ") }
            sb.append(method)
            sb.append(" (")
            sb.append(selfCount)
            sb.append('/')
            sb.append(traceCount)
            sb.appendLine(')')

            if (traceCount > expandUntil) {
                for (node in children.values.sortedWith(compareBy({ -it.traceCount }, { it.method }))) {
                    node.appendTo(sb, level + 1, expandUntil)
                }
            }
        }

        override fun toString(): String {
            return "$method ($selfCount/$traceCount)"
        }
    }

}

class JavaFlightRecorderParser private constructor(
    private val logDirectories: Iterable<Path>
) {

    private fun parse(): Map<Path, StackTrace> {
        // Find all .jfr (Java Flight Recorder) files in given log directories
        val jfrFiles = logDirectories.flatMap { directory ->
            Files.list(directory).use { fileList ->
                fileList.map { it.toFile() }
                    .filter { it.isFile && it.extension == "jfr" }
                    .toList()
            }
        }
        // Parse each recording
        return jfrFiles.associate { jfrFile ->
            // Get the necessary IDs and offset to find stack trace frames among recorded events
            val recording = RecordingFile(jfrFile.toPath())
            val executionSampleEventId = recording.readEventTypes()
                .find { it.name == "jdk.ExecutionSample" }!!.id

            // Read all events, but keep only ExecutionSamples
            val stackTrace = StackTrace()
            val currentStackTrace = arrayListOf<String>()
            while (recording.hasMoreEvents()) {
                val event = recording.readEvent()
                if (event.eventType.id != executionSampleEventId) continue

                currentStackTrace.clear()
                var lastFrameWasNative = false
                var firstFrame = true
                for (frame in event.stackTrace.frames.asReversed()) {
                    val (frameClass, frameMethod) = frame.method.let { m ->
                        m.type.name to m.name
                    }
                    val frameIsNative = frameClass.isEmpty()
                    if (firstFrame || !frameIsNative || !lastFrameWasNative) {
                        if (frameIsNative) {
                            currentStackTrace.add("<native>")
                        } else {
                            currentStackTrace.add("$frameClass.$frameMethod")
                        }
                        firstFrame = false
                        lastFrameWasNative = frameIsNative
                    }
                }
                stackTrace.addTrace(currentStackTrace)
            }

            jfrFile.toPath() to stackTrace
        }
    }

    companion object {

        fun parseFromDirectories(logDirectories: Iterable<Path>): Map<Path, StackTrace> {
            return JavaFlightRecorderParser(logDirectories).parse()
        }

    }

}