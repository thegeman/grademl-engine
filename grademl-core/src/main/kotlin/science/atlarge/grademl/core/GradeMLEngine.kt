package science.atlarge.grademl.core

import java.nio.file.Path

object GradeMLEngine {

    fun analyzeJob(jobDataDirectories: Iterable<Path>, jobOutputDirectory: Path): GradeMLJob {
        return GradeMLJob(jobOutputDirectory)
    }

}