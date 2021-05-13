require(data.table)
require(ggplot2)
require(ggpubr)

phase_list_filename <- "phase-list.tsv"
phase_type_list_filename <- "phase-type-list.tsv"
metric_data_filename <- "metric-data.tsv"
upsampled_metric_data_filename <- "upsampled-metric-data.tsv"
resource_attribution_data_filename <- "resource-attribution-data.tsv"
aggregate_resource_attribution_data_filename <- "aggregate-resource-attribution-data.tsv"

### START OF GENERATED SETTINGS ###
### :setting output_directory   ###
### :setting data_directory     ###
### :setting plot_filename      ###
### :setting metric_id          ###
### :setting plot_per_phase     ###
### :setting start_time         ###
### :setting end_time           ###
### END OF GENERATED SETTINGS   ###

####################
### DATA LOADING ###
####################

# Read phase (type) list
if (plot_per_phase) {
  phase_list <- fread(file = file.path(data_directory, phase_list_filename))[, .(
    phase.id = as.factor(phase.id),
    phase.path = phase.path,
    canonical.index
  )]
} else {
  phase_list <- fread(file = file.path(data_directory, phase_type_list_filename))[, .(
    phase.id = as.factor(phase.type.id),
    phase.path = phase.type.path,
    canonical.index
  )]
}
phase_list <- phase_list[order(canonical.index)][, `:=`(
  canonical.index = NULL,
  phase.path = factor(phase.path, phase.path)
)]

# Read metric data
metric_data <- fread(file = file.path(data_directory, metric_data_filename))[
  metric.id == metric_id, .(timestamp, value)]

# Read upsampled metric data
upsampled_metric_data <- fread(file = file.path(data_directory, upsampled_metric_data_filename))[
  metric.id == metric_id, .(timestamp, value)]

# Read (aggregated) resource attribution data
if (plot_per_phase) {
  resource_attribution_data <- fread(file = file.path(data_directory, resource_attribution_data_filename))[
    metric.id == metric_id, .(phase.id, timestamp, value, count = 1)]
} else {
  resource_attribution_data <- fread(file = file.path(data_directory, aggregate_resource_attribution_data_filename))[
    metric.id == metric_id, .(phase.id = phase.type.id, timestamp, value, count)]
}
resource_attribution_data <- merge(resource_attribution_data, phase_list, by = "phase.id")[,
  .(phase.path, timestamp, value, count)]

##########################
### DATA PREPROCESSING ###
##########################

# Transform metric data to a step function
stepped_metric_data <- metric_data[, .(
  timestamp = c(head(timestamp, -1) + 1, tail(timestamp, -1)),
  value = c(tail(value, -1), tail(value, -1))
)][order(timestamp)]

stepped_upsampled_metric_data <- upsampled_metric_data[, .(
  timestamp = c(head(timestamp, -1) + 1, tail(timestamp, -1)),
  value = c(tail(value, -1), tail(value, -1))
)][order(timestamp)]

stepped_resource_attribution_data <- resource_attribution_data[, .(
  timestamp = c(head(timestamp, -1) + 1, tail(timestamp, -1)),
  value = c(tail(value, -1), tail(value, -1)),
  count = c(tail(count, -1), tail(count, -1))
), by = .(phase.path)][order(phase.path, timestamp)]

# Clamp timestamps between the given start and end time
stepped_metric_data[, timestamp := pmin(pmax(as.double(timestamp), start_time), end_time)]
stepped_upsampled_metric_data[, timestamp := pmin(pmax(as.double(timestamp), start_time), end_time)]
stepped_resource_attribution_data[, timestamp := pmin(pmax(as.double(timestamp), start_time), end_time)]

# Convert timestamps from nanoseconds to seconds
stepped_metric_data[, timestamp := timestamp / 1e9]
stepped_upsampled_metric_data[, timestamp := timestamp / 1e9]
stepped_resource_attribution_data[, timestamp := timestamp / 1e9]

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
phase_count <- length(unique(stepped_resource_attribution_data$phase.path))
p_resource_attribution <- ggplot(stepped_resource_attribution_data)
if (phase_count <= 50) {
  p_resource_attribution <- p_resource_attribution +
    geom_line(data = stepped_upsampled_metric_data, aes(x = timestamp, y = value),
              colour = "gray70", size = 0.05)
}
p_resource_attribution <- p_resource_attribution +
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
