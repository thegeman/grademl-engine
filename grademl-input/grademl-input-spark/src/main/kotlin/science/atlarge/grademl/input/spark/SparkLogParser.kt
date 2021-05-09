package science.atlarge.grademl.input.spark

import java.nio.file.Path

class SparkLogParser private constructor(
    private val sparkLogDirectories: Iterable<Path>
) {

    private fun parse(): SparkLog {
        return SparkLog()
    }

    companion object {

        fun parseFromDirectories(sparkLogDirectories: Iterable<Path>): SparkLog {
            return SparkLogParser(sparkLogDirectories).parse()
        }

    }

}

class SparkLog()