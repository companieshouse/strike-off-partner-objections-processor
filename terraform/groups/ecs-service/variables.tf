# ------------------------------------------------------------------------------
# Environment
# ------------------------------------------------------------------------------
variable "environment" {
  type        = string
  description = "The environment name, defined in envrionments vars."
}
variable "aws_region" {
  default     = "eu-west-2"
  type        = string
  description = "The AWS region for deployment."
}
variable "aws_profile" {
  default     = "development-eu-west-2"
  type        = string
  description = "The AWS profile to use for deployment."
}

# ------------------------------------------------------------------------------
# Docker Container
# ------------------------------------------------------------------------------
variable "docker_registry" {
  type        = string
  description = "The FQDN of the Docker registry."
}

# ------------------------------------------------------------------------------
# Service performance and scaling configs
# ------------------------------------------------------------------------------
variable "desired_task_count" {
  type        = number
  description = "The desired ECS task count for this service"
  default     = 1 # defaulted low for dev environments, override for production
}

variable "min_task_count" {
  type        = number
  description = "The minimum number of tasks for this service."
  default     = 1
}

variable "max_task_count" {
  type        = number
  description = "The maximum number of tasks for this service."
  default     = 3
}
variable "required_cpus" {
  type        = number
  description = "The required cpu resource for this service when running on fargate. 1024 here is 1 vCPU"
  default     = 256
}
variable "required_memory" {
  type        = number
  description = "The required memory for this service when running on fargate"
  default     = 512
}
variable "use_fargate" {
  type        = bool
  description = "If true, sets the required capabilities for all containers in the task definition to use FARGATE, false uses EC2"
  default     = true
}

variable "use_capacity_provider" {
  type        = bool
  description = "Whether to use a capacity provider instead of setting a launch type for the service"
  default     = true
}

variable "service_autoscale_enabled" {
  type        = bool
  description = "Whether to enable service autoscaling, including scheduled autoscaling"
  default     = true
}

variable "service_autoscale_target_value_cpu" {
  type        = number
  description = "Target CPU percentage for the ECS Service to autoscale on"
  default     = 80 # 100 disables autoscaling using CPU as a metric
}

variable "service_scaledown_schedule" {
  type        = string
  description = "The schedule to use when scaling down the number of tasks to zero."
  default     = ""
}

variable "service_scaleup_schedule" {
  type        = string
  description = "The schedule to use when scaling up the number of tasks to their normal desired level."
  default     = ""
}

# Task/container healthcheck tuning for consumer services
variable "task_healthcheck_interval" {
  type        = number
  description = "Time period in seconds between each task/container health check."
}

variable "task_healthcheck_timeout" {
  type        = number
  description = "Time period in seconds to wait before considering a task/container health check failed."
}

variable "task_healthcheck_retries" {
  type        = number
  description = "Number of consecutive failed task/container health checks before marking unhealthy."
}

variable "task_healthcheck_start_period" {
  type        = number
  description = "Grace period in seconds before task/container health check failures are counted."
}

# ----------------------------------------------------------------------
# Cloudwatch alerts
# ----------------------------------------------------------------------
variable "cloudwatch_alarms_enabled" {
  description = "Whether to create a standard set of cloudwatch alarms for the service.  Requires an SNS topic to have already been created for the stack."
  type        = bool
  default     = false
}
# ------------------------------------------------------------------------------
# Service environment variable configs
# ------------------------------------------------------------------------------
variable "ssm_version_prefix" {
  type        = string
  description = "String to use as a prefix to the names of the variables containing variables and secrets version."
  default     = "SSM_VERSION_"
}

variable "use_set_environment_files" {
  type        = bool
  default     = true
  description = "Toggle default global and shared  environment files"
}

variable "strike_off_partner_objections_processor_version" {
  type        = string
  description = "The version of the strike_off_partner_objections_processor container to run."
}

# ------------------------------------------------------------------------------
# KAFKA ERROR CONSUMER configuration
# ------------------------------------------------------------------------------
variable "enable_scale_down_eventbridge_scheduler" {
  default     = false
  description = "Whether to enable the scale down EventBridge scheduler for the ECS service"
  type        = bool
}

variable "enable_scale_up_eventbridge_scheduler" {
  default     = false
  description = "Whether to enable the scale up EventBridge scheduler for the ECS service"
  type        = bool
}

variable "startup_eventbridge_scheduler_cron" {
  description = "Cron expression for the startup scheduler"
  type        = string
  default     = ""
}

variable "shutdown_eventbridge_scheduler_cron" {
  description = "Cron expression for shutdown scheduler"
  type        = string
  default     = ""
}

variable "kafka_error_consumer_version" {
  type        = string
  description = "The version of the kafka_error_consumer container to run."
  default     = ""
}

variable "desired_task_count_kafka_error" {
  type        = number
  description = "The desired ECS task count for this service"
  default     = 0 # defaulted low for dev environments, override for production
}

variable "min_task_count_kafka_error" {
  type        = number
  description = "The minimum number of tasks for this service."
  default     = 0
}

variable "max_task_count_kafka_error" {
  type        = number
  description = "The maximum number of tasks for this service."
  default     = 1
}

variable "service_autoscale_scale_in_cooldown" {
  type        = number
  description = "Cooldown in seconds for ECS Service scale in (run fewer tasks)"
  default     = 600
}

variable "service_autoscale_scale_out_cooldown" {
  type        = number
  description = "Cooldown in seconds for ECS Service scale out (add more tasks)"
  default     = 600
}

