package science.atlarge.grademl.cli

import org.jline.builtins.Completers
import org.jline.reader.LineReaderBuilder
import org.jline.reader.impl.DefaultParser
import org.jline.reader.impl.history.DefaultHistory
import org.jline.terminal.TerminalBuilder
import science.atlarge.grademl.cli.util.PhaseList
import science.atlarge.grademl.core.execution.ExecutionModel
import science.atlarge.grademl.core.resources.ResourceModel
import science.atlarge.grademl.input.airflow.Airflow
import science.atlarge.grademl.input.resource_monitor.ResourceMonitor
import java.io.IOError
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.system.exitProcess

object Cli {

    @JvmStatic
    fun main(args: Array<String>) {
        println("Welcome to the GradeML CLI!")
        println()

        if (args.size != 2) {
            println("Usage: grademl-cli <jobLogDirectory> <jobAnalysisDirectory>")
            exitProcess(1)
        }

        println("Parsing job log files.")
        val (executionModel, resourceModel) = parseJobLogs(Paths.get(args[0]))
        println("Completed parsing of input files.")
        println()

        println("Explore the job's performance data interactively by issuing commands.")
        println("Enter \"help\" for a list of available commands.")
        println()

        runCli(CliState(executionModel, resourceModel, Paths.get(args[1])))
    }

    private fun parseJobLogs(jobLogDirectory: Path): Pair<ExecutionModel, ResourceModel> {
        val executionModel = ExecutionModel()
        val resourceModel = ResourceModel()

        Airflow.parseJobLogs(jobLogDirectory, executionModel)
        ResourceMonitor.parseJobLogs(jobLogDirectory, resourceModel)

        return executionModel to resourceModel
    }

    private fun runCli(cliState: CliState) {
        // Set up the terminal and CLI parsing library
        val terminal = TerminalBuilder.builder()
            .jansi(true)
            .build()
        val completer = Completers.AnyCompleter.INSTANCE
        val parser = DefaultParser()
        val lineReader = LineReaderBuilder.builder()
            .appName("GradeML")
            .terminal(terminal)
            .completer(completer)
            .parser(parser)
            .history(DefaultHistory())
            .build()

        // Repeatedly read, parse, and execute commands until the users quits the application
        while (true) {
            // Read the next line
            val line = try {
                lineReader.readLine("> ")
            } catch (e: Exception) {
                when (e) {
                    // End the CLI when the line reader is aborted
                    is IOError -> break
                    else -> throw e
                }
            }

            // For now, just echo back the parsed line
            val parsedLine = lineReader.parser.parse(line, 0)
            if (parsedLine.line().isBlank()) {
                // Skip empty lines
                continue
            }
            // Look up the first word as command
            val command = CommandRegistry[parsedLine.words()[0]]
            if (command == null) {
                println("Command \"${parsedLine.words()[0]}\" not recognized.")
                println()
                continue
            }
            // Invoke the command
            command.process(parsedLine.words().drop(1), cliState)
            println()
        }
    }

}

class CliState(
    val executionModel: ExecutionModel,
    val resourceModel: ResourceModel,
    val outputPath: Path
) {

    val phaseList = PhaseList.fromExecutionModel(executionModel)

}