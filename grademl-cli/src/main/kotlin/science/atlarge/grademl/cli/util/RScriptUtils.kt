package science.atlarge.grademl.cli.util

import java.io.InputStream
import java.nio.file.Path

fun writeRScript(sourceStream: InputStream, destination: Path, settings: Map<String, String> = emptyMap()) {
    val SETTING_REGEX = """^\s*#+\s*:setting\s+([^\s]+)(?:\s+.*)$""".toRegex()
    sourceStream.bufferedReader().useLines { inputLines ->
        destination.toFile().bufferedWriter().use { writer ->
            inputLines.forEach { line ->
                val settingMatch = SETTING_REGEX.matchEntire(line)
                if (settingMatch == null) {
                    writer.appendLine(line)
                } else {
                    val settingName = settingMatch.groupValues[1]
                    val settingValue = settings[settingName] ?: throw IllegalArgumentException(
                            "Missing value for setting $settingName")
                    writer.appendLine("$settingName <- $settingValue")
                }
            }
        }
    }
}