output "event_queue_url" {
  value = aws_sqs_queue.events.url
}

output "dead_letter_queue_url" {
  value = aws_sqs_queue.dead_letter.url
}

output "event_table_name" {
  value = aws_dynamodb_table.events.name
}
