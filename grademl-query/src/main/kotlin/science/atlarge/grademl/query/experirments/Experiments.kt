package science.atlarge.grademl.query.experirments

import science.atlarge.grademl.core.GradeMLEngine
import science.atlarge.grademl.input.airflow.Airflow
import science.atlarge.grademl.input.resource_monitor.ResourceMonitor
import science.atlarge.grademl.input.spark.Spark
import science.atlarge.grademl.input.tensorflow.TensorFlow
import java.io.File
import java.nio.file.Paths
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    if (args.size != 2) {
        println("Usage: grademl-experiments <jobLogDirectories> <experimentOutputDirectory>")
        println("  jobLogDirectories must be separated by the ${File.pathSeparatorChar} character")
        exitProcess(1)
    }

    val inputPaths = args[0].split(File.pathSeparatorChar).map { Paths.get(it) }
    val outputPath = Paths.get(args[1])

//    GradeMLEngine.registerInputSource(ResourceMonitor)
    GradeMLEngine.registerInputSource(Spark)
    GradeMLEngine.registerInputSource(TensorFlow)
    GradeMLEngine.registerInputSource(Airflow)

    exportJobCharacteristics(inputPaths, outputPath)
//    runResourceAttributionOverheadExperiment(inputPaths, outputPath)
}