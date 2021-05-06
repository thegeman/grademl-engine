description = "Basic definitions of GradeML data model"

plugins {
    kotlin("jvm") version "1.5.0-RC"
}

sourceSets {
    main {
        resources {
            srcDir("src/main/R")
        }
    }
}