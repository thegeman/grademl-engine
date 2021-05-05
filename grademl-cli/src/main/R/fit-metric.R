library(data.table)
library(bvls)

phase_list_filename <- "phase-list.tsv"
metric_list_filename <- "metric-list.tsv"
metric_data_filename <- "metric-data.tsv"

### START OF GENERATED SETTINGS ###
### :setting output_directory   ###
### :setting data_directory     ###
### END OF GENERATED SETTINGS   ###

# Read phase list
phase_list <- fread(file = paste0(data_directory, phase_list_filename))
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
metric_list <- fread(file = paste0(data_directory, metric_list_filename))
metric_list <- metric_list[, .(
  metric.id = as.factor(metric.id),
  metric.path = as.factor(metric.path),
  max.value = as.double(max.value)
)]

# Read metric data
metric_data <- fread(file = paste0(data_directory, metric_data_filename))[, .(
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

# Transform metric data from timestamped events to time periods
metric_data_periods <- metric_data[, .(
  time.period.id = 1:(.N-1),
  timestamp.from = head(timestamp, -1),
  timestamp.to = tail(timestamp, -1),
  metric.value = tail(value, -1)
)]

# Filter out leaf phases
phase_list[phase.id == parent.phase.id]$parent.phase.id <- "NA"
leaf_phase_list <- phase_list[!(phase.id %in% phase_list$parent.phase.id)]

# Cross-join the metric data and phase list to create one data point per time period per phase
metric_per_period_per_phase <- metric_data_periods[, .(
  phase.from = leaf_phase_list$start.time,
  phase.to = leaf_phase_list$end.time,
  phase.id = leaf_phase_list$phase.id
), by = metric_data_periods][, .(
  time.period.id,
  phase.id,
  phase.activity = pmax(
    (timestamp.to - timestamp.from) - pmax(phase.from - timestamp.from, 0) - pmax(timestamp.to - phase.to, 0),
    0
  ) / (timestamp.to - timestamp.from)
)]

# Cast metric data to a matrix
metric_matrix <- dcast(metric_per_period_per_phase, time.period.id ~ phase.id, value.var = "phase.activity")
metric_matrix <- as.matrix(metric_matrix[order(time.period.id)][, time.period.id := NULL])

# Create vector of observed output values
value_vector <- metric_data_periods[order(time.period.id)]$metric.value

# Fit a linear relationship to observed metric values and phase activities
fit_result <- bvls(metric_matrix, value_vector, bl = rep(0, ncol(metric_matrix)), bu = rep(Inf, ncol(metric_matrix)))

# Produce a final list of coefficients per phase
phase_coefficients <- data.table(phase.id = colnames(metric_matrix), phase.coefficient = fit_result$x)
phase_coefficients <- merge(phase_coefficients, phase_list)[, .(phase.id, phase.path, phase.coefficient)]
