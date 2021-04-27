package science.atlarge.grademl.cli.util

import science.atlarge.grademl.cli.Cli
import java.io.File
import java.io.InputStream
import java.nio.file.Path

fun instantiateRScript(rScriptFile: File, settings: Map<String, String> = emptyMap()) {
    val rScriptStream = Cli.javaClass.getResourceAsStream("/${rScriptFile.name}")!!
    if (rScriptFile.exists()) {
        rScriptFile.delete()
    }
    writeRScript(rScriptStream, rScriptFile, settings)
}

fun writeRScript(sourceStream: InputStream, destination: File, settings: Map<String, String> = emptyMap()) {
    val SETTING_REGEX = """^\s*#+\s*:setting\s+([^\s]+)(?:\s+.*)$""".toRegex()
    sourceStream.bufferedReader().useLines { inputLines ->
        destination.bufferedWriter().use { writer ->
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

fun runRScript(rScriptFile: File) {
    val rScriptExec = System.getenv().getOrElse("RSCRIPT") { "Rscript" }
    val rScriptDirectory = rScriptFile.parentFile

    val pb = ProcessBuilder(rScriptExec, rScriptFile.absolutePath)
    pb.directory(rScriptDirectory)
    pb.redirectErrorStream(true)
    pb.redirectOutput(rScriptDirectory.resolve(rScriptFile.nameWithoutExtension + ".log"))
    pb.start().waitFor()
}