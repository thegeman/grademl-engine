require(data.table)
require(ggplot2)
require(ggpubr)

phase_list_filename <- "phase-list.tsv"
metric_list_filename <- "metric-list.tsv"
metric_data_filename <- "metric-data.tsv"
upsampled_metric_data_filename <- "upsampled-metric-data.tsv"
resource_attribution_data_filename <- "resource-attribution-data.tsv"

### START OF GENERATED SETTINGS ###
### :setting output_directory   ###
### :setting data_directory     ###
### :setting plot_filename      ###
### :setting metric_id          ###
### END OF GENERATED SETTINGS   ###

# Read phase list
phase_list <- fread(file = file.path(data_directory, phase_list_filename))
phase_list <- phase_list[, .(
  phase.id = as.factor(phase.id),
  phase.path = as.factor(phase.path),
  parent.phase.id = as.factor(parent.phase.id),
  depth = depth,
  start.time = start.time.ns / 1e9,
  end.time = end.time.ns / 1e9,
  canonical.index
)][, `:=`(
  parent.phase.id = factor(parent.phase.id, levels(phase.id))
)][order(-canonical.index)][, `:=`(
  order.num = 0:(.N - 1)
)]

# Read metric list
metric_list <- fread(file = file.path(data_directory, metric_list_filename))
metric_list <- metric_list[, .(
  metric.id = as.factor(metric.id),
  metric.path = as.factor(metric.path),
  max.value = as.double(max.value)
)]

# Read metric data
metric_data <- fread(file = file.path(data_directory, metric_data_filename))[, .(
  metric.id,
  timestamp = timestamp / 1e9,
  value
)]
metric_data <- merge(metric_data, metric_list, by = "metric.id")[, .(
  metric.id,
  metric.path,
  timestamp,
  value,
  max.value
)]

# Read upsampled metric data
upsampled_metric_data <- fread(file = file.path(data_directory, upsampled_metric_data_filename))[, .(
  metric.id,
  timestamp = timestamp / 1e9,
  value
)]
upsampled_metric_data <- merge(upsampled_metric_data, metric_list, by = "metric.id")[, .(
  metric.id,
  metric.path,
  timestamp,
  value,
  max.value
)]

# Read resource attribution data
resource_attribution_data <- fread(file = file.path(data_directory, resource_attribution_data_filename))[, .(
  metric.id,
  phase.id,
  timestamp = timestamp / 1e9,
  value
)]
resource_attribution_data <- merge(resource_attribution_data, metric_list, by = "metric.id")[, .(
  metric.id,
  metric.path,
  phase.id,
  timestamp,
  value
)]
resource_attribution_data <- merge(resource_attribution_data, phase_list, by = "phase.id")[, .(
  metric.id,
  metric.path,
  phase.id,
  phase.path,
  timestamp,
  value
)]
