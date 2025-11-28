# Asset Validation EventBridge Scheduler Configuration

## Overview

This document describes how to configure AWS EventBridge Scheduler to trigger the Asset Validation Worker via SQS queue every minute.

## Architecture Flow

```
EventBridge Scheduler (every 1 minute)
    ↓
SQS Queue (AWS_SQS_ASSET_VALIDATION_QUEUE_URL)
    ↓
Fargate Service (asset_validation_worker)
    ↓
Scans DynamoDB for VALID_STRUCTURE packages
    ↓
Processes each package (CSV validation, checksum, etc.)
    ↓
Moves files to Success (Asset Repo) or Error location
```

## EventBridge Scheduler Configuration

### Schedule Expression
```
rate(1 minute)
```

### Target Configuration

**Target Type:** SQS Queue

**Queue ARN:** Your SQS queue ARN for asset validation

**Message Body (Input):**
```json
{
  "trigger": "asset_validation_check"
}
```

### Complete EventBridge Scheduler Settings

#### Using AWS Console:

1. **Name:** `asset-validation-scheduler`
2. **Description:** Triggers asset validation worker every minute to check for packages requiring validation
3. **Schedule Pattern:** Rate-based schedule
4. **Rate Expression:** `rate(1 minute)`
5. **Flexible time window:** Off
6. **Target API:** SQS SendMessage
7. **SQS Queue:** Select your asset validation queue
8. **Message Body:**
   ```json
   {
     "trigger": "asset_validation_check"
   }
   ```
9. **Retry Policy:**
   - Maximum age of event: 1 hour
   - Retry attempts: 2

#### Using AWS CLI:

```bash
aws scheduler create-schedule \
  --name asset-validation-scheduler \
  --schedule-expression "rate(1 minute)" \
  --flexible-time-window Mode=OFF \
  --target '{
    "Arn": "arn:aws:sqs:us-east-1:YOUR_ACCOUNT_ID:asset-validation-queue",
    "RoleArn": "arn:aws:iam::YOUR_ACCOUNT_ID:role/EventBridgeSchedulerRole",
    "Input": "{\"trigger\": \"asset_validation_check\"}"
  }'
```

#### Using Terraform:

```hcl
resource "aws_scheduler_schedule" "asset_validation" {
  name = "asset-validation-scheduler"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "rate(1 minute)"

  target {
    arn      = aws_sqs_queue.asset_validation_queue.arn
    role_arn = aws_iam_role.eventbridge_scheduler_role.arn

    input = jsonencode({
      trigger = "asset_validation_check"
    })

    retry_policy {
      maximum_event_age_in_seconds = 3600
      maximum_retry_attempts       = 2
    }
  }
}
```

#### Using CloudFormation:

```yaml
AssetValidationScheduler:
  Type: AWS::Scheduler::Schedule
  Properties:
    Name: asset-validation-scheduler
    Description: Triggers asset validation worker every minute
    ScheduleExpression: rate(1 minute)
    FlexibleTimeWindow:
      Mode: "OFF"
    Target:
      Arn: !GetAtt AssetValidationQueue.Arn
      RoleArn: !GetAtt EventBridgeSchedulerRole.Arn
      Input: '{"trigger": "asset_validation_check"}'
      RetryPolicy:
        MaximumEventAgeInSeconds: 3600
        MaximumRetryAttempts: 2
```

## Required IAM Policy for EventBridge Scheduler Role

The EventBridge Scheduler needs permission to send messages to your SQS queue:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sqs:SendMessage"
      ],
      "Resource": "arn:aws:sqs:us-east-1:YOUR_ACCOUNT_ID:asset-validation-queue"
    }
  ]
}
```

## Environment Variables Required

Ensure these environment variables are configured in your Fargate task:

```env
# Required for worker
AWS_SQS_ASSET_VALIDATION_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/YOUR_ACCOUNT_ID/asset-validation-queue
INGEST_S3_BUCKET=routerunner-poc-ingest
ASSET_REPO_S3_BUCKET=routerunner-poc-asset-repo
INGEST_ASSET_TABLE=routerunner-poc-ingest-asset
TITLE_INFO_TABLE=routerunner-poc-title-info
ASSET_INFO_TABLE=routerunner-poc-asset-info
ASSET_VALIDATION_CUTOFF_MINUTES=1
SERVICE_TYPE=ASSET_VALIDATION_WORKER

# AWS Configuration
AWS_REGION=us-east-1
AWS_DEFAULT_REGION=us-east-1
```

## Message Format Details

### EventBridge Scheduler Message (Input to SQS)

```json
{
  "trigger": "asset_validation_check"
}
```

**Field Descriptions:**
- `trigger`: Must be `"asset_validation_check"` - This tells the worker to scan for VALID_STRUCTURE packages

### Worker Behavior

When the worker receives this message:

1. **Validates trigger type:** Ensures `trigger == "asset_validation_check"`
2. **Calculates cutoff time:** `now - ASSET_VALIDATION_CUTOFF_MINUTES`
3. **Scans DynamoDB:** Queries all items with `ProcessStatus = "VALID_STRUCTURE"`
4. **Filters by age:** Only processes packages older than cutoff time
5. **Processes each package:**
   - Checks for CSV presence
   - Validates CSV structure and content
   - Compares files against CSV manifest
   - Validates checksums (if present)
   - Moves files to appropriate locations (Success/Error)
   - Updates DynamoDB status
6. **Reports results:** Logs processed/failed counts

## Testing the Configuration

### 1. Test EventBridge Scheduler Manually

Send a test message to your SQS queue:

```bash
aws sqs send-message \
  --queue-url https://sqs.us-east-1.amazonaws.com/YOUR_ACCOUNT_ID/asset-validation-queue \
  --message-body '{"trigger": "asset_validation_check"}'
```

### 2. Monitor Worker Logs

Check CloudWatch Logs for your Fargate service:

```bash
aws logs tail /ecs/asset-validation-worker --follow
```

Expected log output:
```
Asset validation check triggered - scanning for VALID_STRUCTURE packages
Found X packages with VALID_STRUCTURE status
Processing package: TitleID/AssetID (created: 2025-01-15T10:30:00+00:00)
...
Validation check complete: X processed, Y failed
```

### 3. Verify SQS Queue

Check queue metrics in AWS Console:
- Messages sent
- Messages received
- Messages deleted
- Approximate age of oldest message

### 4. Verify DynamoDB Updates

Query packages to verify status updates:

```bash
aws dynamodb query \
  --table-name routerunner-poc-ingest-asset \
  --index-name ProcessStatusIndex \
  --key-condition-expression "ProcessStatus = :status" \
  --expression-attribute-values '{":status":{"S":"SUCCESS"}}'
```

## Troubleshooting

### Worker not processing messages

1. **Check queue URL:** Ensure `AWS_SQS_ASSET_VALIDATION_QUEUE_URL` is correct
2. **Check IAM permissions:** Fargate task role needs SQS permissions
3. **Check worker logs:** Look for initialization errors
4. **Check message format:** Ensure message contains `{"trigger": "asset_validation_check"}`

### Messages stuck in queue

1. **Check visibility timeout:** Should be > 300 seconds (5 minutes)
2. **Check worker health:** Ensure Fargate service is running
3. **Check for errors:** Review CloudWatch Logs for exceptions
4. **Check DLQ:** Messages with repeated failures go to DLQ

### EventBridge Scheduler not triggering

1. **Check schedule state:** Ensure schedule is ENABLED
2. **Check IAM role:** EventBridge needs SQS SendMessage permission
3. **Check schedule expression:** Verify `rate(1 minute)` is correct
4. **Check target configuration:** Ensure queue ARN is correct

## Monitoring and Alerts

### CloudWatch Metrics to Monitor

1. **SQS Queue Metrics:**
   - `ApproximateNumberOfMessagesVisible`
   - `ApproximateAgeOfOldestMessage`
   - `NumberOfMessagesSent`
   - `NumberOfMessagesReceived`

2. **ECS Service Metrics:**
   - `CPUUtilization`
   - `MemoryUtilization`
   - Task health

3. **DynamoDB Metrics:**
   - `ConsumedReadCapacityUnits`
   - `ConsumedWriteCapacityUnits`

### Recommended CloudWatch Alarms

```yaml
# SQS Queue Depth Alarm
MetricName: ApproximateNumberOfMessagesVisible
Threshold: 100
ComparisonOperator: GreaterThanThreshold
EvaluationPeriods: 2

# Old Message Age Alarm
MetricName: ApproximateAgeOfOldestMessage
Threshold: 600  # 10 minutes
ComparisonOperator: GreaterThanThreshold
EvaluationPeriods: 1

# ECS Service Unhealthy Alarm
MetricName: HealthyTaskCount
Threshold: 1
ComparisonOperator: LessThanThreshold
EvaluationPeriods: 2
```

## Cost Optimization

### Adjusting Schedule Frequency

If validation doesn't need to run every minute, adjust the rate:

```
rate(5 minutes)  # Every 5 minutes
rate(15 minutes) # Every 15 minutes
```

### SQS Cost Considerations

- EventBridge sends ~1,440 messages/day (every minute)
- Consider batching or adjusting schedule frequency for cost savings

## Migration from Lambda

### Differences from Lambda Implementation

| Aspect | Lambda (Old) | Fargate Worker (New) |
|--------|-------------|---------------------|
| Trigger | EventBridge → Lambda directly | EventBridge → SQS → Fargate |
| Execution | Scheduled function runs | Continuous polling worker |
| Scaling | Lambda auto-scales | Fargate task count |
| Timeout | 15 minutes max | No timeout (long-running) |
| Cost | Pay per invocation | Pay for running time |
| Cold starts | Yes | No (always warm) |

### Migration Steps

1. **Deploy Fargate service** with asset_validation_worker
2. **Create SQS queue** for asset validation
3. **Configure EventBridge Scheduler** to send to SQS instead of Lambda
4. **Test with both running** to verify behavior
5. **Monitor for 24 hours** to ensure stability
6. **Disable Lambda** EventBridge trigger
7. **Delete Lambda function** after verification period

## Example Complete Setup Script

```bash
#!/bin/bash

# Variables
ACCOUNT_ID="YOUR_ACCOUNT_ID"
REGION="us-east-1"
QUEUE_NAME="asset-validation-queue"

# Create SQS Queue
aws sqs create-queue \
  --queue-name ${QUEUE_NAME} \
  --attributes '{
    "VisibilityTimeout": "300",
    "MessageRetentionPeriod": "3600",
    "ReceiveMessageWaitTimeSeconds": "20"
  }'

QUEUE_URL=$(aws sqs get-queue-url --queue-name ${QUEUE_NAME} --query 'QueueUrl' --output text)
QUEUE_ARN="arn:aws:sqs:${REGION}:${ACCOUNT_ID}:${QUEUE_NAME}"

# Create EventBridge Scheduler IAM Role
aws iam create-role \
  --role-name EventBridgeSchedulerAssetValidationRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "scheduler.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'

# Attach SQS policy
aws iam put-role-policy \
  --role-name EventBridgeSchedulerAssetValidationRole \
  --policy-name SQSSendMessagePolicy \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [{
      \"Effect\": \"Allow\",
      \"Action\": \"sqs:SendMessage\",
      \"Resource\": \"${QUEUE_ARN}\"
    }]
  }"

# Create EventBridge Scheduler
aws scheduler create-schedule \
  --name asset-validation-scheduler \
  --schedule-expression "rate(1 minute)" \
  --flexible-time-window Mode=OFF \
  --target "{
    \"Arn\": \"${QUEUE_ARN}\",
    \"RoleArn\": \"arn:aws:iam::${ACCOUNT_ID}:role/EventBridgeSchedulerAssetValidationRole\",
    \"Input\": \"{\\\"trigger\\\": \\\"asset_validation_check\\\"}\"
  }"

echo "Setup complete!"
echo "Queue URL: ${QUEUE_URL}"
echo "Update your Fargate task environment variable:"
echo "AWS_SQS_ASSET_VALIDATION_QUEUE_URL=${QUEUE_URL}"
```

## Summary

The asset validation worker is now configured to work with EventBridge Scheduler via SQS, providing:

- ✅ Scheduled validation checks every minute
- ✅ Decoupled architecture (EventBridge → SQS → Worker)
- ✅ Scalable processing with Fargate
- ✅ Reliable message delivery with SQS
- ✅ Easy monitoring and debugging
- ✅ No Lambda timeout limitations
