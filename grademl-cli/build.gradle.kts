description = "CLI for GradeML"

plugins {
    kotlin("jvm") version "1.5.0-RC"
    application
}

application {
    mainClass.set("science.atlarge.grademl.cli.Cli")
}

dependencies {
    implementation(project(":grademl-core"))
    implementation(project(":grademl-input:grademl-input-airflow"))
    implementation(project(":grademl-input:grademl-input-resource-monitor"))

    implementation("org.jline:jline:3.19.0")
    runtimeOnly("org.fusesource.jansi:jansi:2.3.2")
}

sourceSets {
    main {
        resources {
            srcDir("src/main/R")
        }
    }
}