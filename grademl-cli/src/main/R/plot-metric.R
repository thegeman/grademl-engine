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

####################
### DATA LOADING ###
####################

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

##########################
### DATA PREPROCESSING ###
##########################

# Transform metric data to a step function
stepped_metric_data <- metric_data[, .(
  timestamp = c(head(timestamp, -1) + 1e-9, tail(timestamp, -1)),
  value = c(tail(value, -1), tail(value, -1))
), by = .(metric.id, metric.path, max.value)][order(metric.id, timestamp)]

stepped_upsampled_metric_data <- upsampled_metric_data[, .(
  timestamp = c(head(timestamp, -1) + 1e-9, tail(timestamp, -1)),
  value = c(tail(value, -1), tail(value, -1))
), by = .(metric.id, metric.path, max.value)][order(metric.id, timestamp)]

stepped_resource_attribution_data <- resource_attribution_data[, .(
  timestamp = c(head(timestamp, -1) + 1e-9, tail(timestamp, -1)),
  value = c(tail(value, -1), tail(value, -1))
), by = .(metric.id, metric.path, phase.id, phase.path)][order(metric.id, phase.id, timestamp)]

################
### PLOTTING ###
################

# Determine the start and end time of the plot
start_time <- min(stepped_metric_data$timestamp)
end_time <- max(stepped_metric_data$timestamp)

# Plot observed resource usage
p_observed_metric <- ggplot(stepped_metric_data) +
  geom_line(aes(x = timestamp, y = value)) +
  scale_x_continuous(limits = c(start_time, end_time), expand = c(0, 0)) +
  expand_limits(y = c(0, 1)) +
  ggtitle("Observed resource usage") +
  theme_bw()

# Plot upsampled resource usage
p_upsampled_metric <- ggplot(stepped_upsampled_metric_data) +
  geom_line(aes(x = timestamp, y = value)) +
  scale_x_continuous(limits = c(start_time, end_time), expand = c(0, 0)) +
  expand_limits(y = c(0, 1)) +
  ggtitle("Upsampled resource usage") +
  theme_bw()

# Plot resource attribution data
phase_count <- length(unique(stepped_resource_attribution_data$phase.id))
p_resource_attribution <- ggplot(stepped_resource_attribution_data) +
  geom_line(data = stepped_upsampled_metric_data, aes(x = timestamp, y = value),
            colour = "gray70", size = 0.05) +
  geom_line(aes(x = timestamp, y = value), size = 0.25) +
  scale_x_continuous(limits = c(start_time, end_time), expand = c(0, 0)) +
  expand_limits(y = c(0, 1)) +
  facet_wrap(~ phase.path, scales = "free_y", nrow = phase_count) +
  ggtitle("Attributed resource usage") +
  theme_bw()

# Arrange the different subplots in one plot
p_observed_metric_height <- 6
p_upsampled_metric_height <- 6
p_resource_attribution_height <- 4 * phase_count + 2
p <- ggarrange(p_observed_metric, p_upsampled_metric, p_resource_attribution,
               heights = c(p_observed_metric_height,
                           p_upsampled_metric_height,
                           p_resource_attribution_height),
               ncol = 1, nrow = 3, align = "v")

# Save the plot
ggsave(file.path(output_directory, plot_filename), width = 40,
       height = p_observed_metric_height + p_upsampled_metric_height +
         p_resource_attribution_height,
       units = "cm", limitsize = FALSE, p)
