require(data.table)
require(ggplot2)
require(ggpubr)

output_directory <- "../"
data_directory <- "../.data/"
phase_list_filename <- "phase-list.tsv"
metric_list_filename <- "metric-list.tsv"
metric_data_filename <- "metric-data.tsv"

### START OF GENERATED SETTINGS ###
### :setting plot_filename      ###
### END OF GENERATED SETTINGS   ###

# Read phase list
phase_list <- fread(paste0(data_directory, phase_list_filename))
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
metric_list <- fread(paste0(data_directory, metric_list_filename))
metric_list <- metric_list[, .(
  metric.id = as.factor(metric.id),
  metric.path = as.factor(metric.path),
  max.value = as.double(max.value)
)]

# Read metric data
metric_data <- fread(paste0(data_directory, metric_data_filename))[, .(
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

# Transform metric data to a step function
stepped_metric_data <- rbind(metric_data, metric_data[, .(
  timestamp = head(timestamp, -1) + 1e-6,
  value = tail(value, -1)
), by = .(metric.id, metric.path, max.value)])[order(metric.id, timestamp)]

# Determine the start and end time of the plot
start_time <- min(min(stepped_metric_data$timestamp), min(phase_list$start.time))
end_time <- max(max(stepped_metric_data$timestamp), max(phase_list$end.time))

# Format each phase's path to shorten every non-leaf component to one character
format_phase_label <- function(phase_path) {
  path_components <- unlist(strsplit(as.character(phase_path), "/", fixed = TRUE))
  if (length(path_components) < 3) {
    return(as.character(phase_path))
  }
  new_path_components <- c("")
  for (i in 2:(length(path_components) -  1)) {
    new_path_components[i] <- substr(path_components[i], 1, 1)
  }
  new_path_components[length(path_components)] <- path_components[length(path_components)]
  return(paste(new_path_components, collapse = "/"))
}

# Translate each phase into a rectangle to be plotted
phase_count <- nrow(phase_list)
max_depth <- max(phase_list$depth)
min_rect_height <- 1
rect_height_increment <- 0.2
max_rect_height <- min_rect_height + max_depth * rect_height_increment
rect_margin <- max_rect_height * 0.25
rect_vertical_step <- max_rect_height + rect_margin
phase_rectangles <- phase_list[, .(
  xmin = start.time,
  xmax = end.time,
  ycenter = rect_vertical_step * (order.num + 0.5),
  height = max_rect_height - depth * rect_height_increment,
  ylabel = lapply(phase.path, format_phase_label),
  depth
)][, .(
  xmin,
  xmax,
  ymin = ycenter - height / 2,
  ymax = ycenter + height / 2,
  ycenter,
  ylabel,
  depth
)]

# Plot the phase rectangles
p_phase <- ggplot(phase_rectangles) +
  geom_rect(aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax, fill = factor(depth, levels = 0:max_depth)),
            colour = "black") +
  geom_label(aes(x = (min(phase_rectangles$xmin) + max(phase_rectangles$xmax)) / 2,
                 y = phase_rectangles$ycenter, label = phase_rectangles$ylabel),
             label.size = NA, fill = "#FFFFFF80") +
  xlab("Time [s]") +
  ylab("Phases") +
  scale_x_continuous(
    expand = c(0, 0),
    limits = c(start_time, end_time)
  ) +
  scale_y_continuous(
    breaks = phase_rectangles$ycenter,
    labels = NULL,
    minor_breaks = NULL,
    limits = c(0, phase_count * rect_vertical_step),
    expand = c(0, 0)
  ) +
  scale_fill_discrete(guide = FALSE) +
  theme_bw() +
  theme(
    axis.text.y = element_text(hjust = 0)
  )

# Plot consumable resource usage
resource_count <- length(unique(metric_data$metric.id))
resource_blanks <- metric_data[, .(
  timestamp = timestamp[1],
  value = max.value[1]
), by = .(metric.path)]

p_resource <- ggplot(stepped_metric_data) +
  geom_line(aes(x = timestamp, y = value)) +
  geom_blank(data = resource_blanks, aes(x = timestamp, y = value)) +
  scale_x_continuous(limits = c(start_time, end_time), expand = c(0, 0)) +
  facet_wrap(~ metric.path, scales = "free_y", nrow = resource_count) +
  theme_bw()

# Arrange the phases and metrics in one plot
p <- ggarrange(p_phase, p_resource, heights = c(1 * phase_count + 3, 4 * resource_count + 3),
               ncol = 1, nrow = 2, align = "v")

# Save the plot
ggsave(paste0(output_directory, plot_filename), width = 40,
       height = 1 * phase_count + 3 + 4 * resource_count + 3,
       units = "cm", limitsize = FALSE, p)