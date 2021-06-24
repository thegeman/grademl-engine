rootProject.name = "grademl-engine"

include(":grademl-core")
include(":grademl-input:grademl-input-airflow")
include(":grademl-input:grademl-input-resource-monitor")
include(":grademl-input:grademl-input-spark")
include(":grademl-query")
include(":grademl-cli")