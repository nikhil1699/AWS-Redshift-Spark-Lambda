# AWS-Redshift-Spark-Lambda
Code to delete card details stored in redshift using Lambda, Spark and SQS
Use AWS Lambda to trigger the deletion process.
Lambda function puts the credit card IDs into an Amazon Simple Queue Service (SQS) queue.
An Amazon EMR cluster with Spark is launched to process the deletion job.
Spark reads the credit card IDs from the SQS queue and performs the deletion in parallel.

Instead of sending individual credit card IDs to the SQS queue one by one, consider batching the IDs in groups for more efficient processing.
In the Lambda function, accumulate a certain number of credit card IDs in a batch and send them as a single message to the SQS queue. This reduces the number of API calls and improves throughput.

Implement a queue-based approach in the Lambda function to handle backpressure and throttle the rate of sending messages to SQS based on the consumption rate by the Spark job.
