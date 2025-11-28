# Asset Validation Worker - Quick Start Guide

## 🚀 Quick Setup

### 1. EventBridge Scheduler Message Format

Configure your EventBridge Scheduler to send this exact message to the SQS queue:

```json
{
  "trigger": "asset_validation_check"
}
```

### 2. Schedule Expression

```
rate(1 minute)
```

### 3. Required Environment Variables

```env
AWS_SQS_ASSET_VALIDATION_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/ACCOUNT_ID/asset-validation-queue
INGEST_S3_BUCKET=routerunner-poc-ingest
ASSET_REPO_S3_BUCKET=routerunner-poc-asset-repo
INGEST_ASSET_TABLE=routerunner-poc-ingest-asset
TITLE_INFO_TABLE=routerunner-poc-title-info
ASSET_INFO_TABLE=routerunner-poc-asset-info
ASSET_VALIDATION_CUTOFF_MINUTES=1
SERVICE_TYPE=ASSET_VALIDATION_WORKER
AWS_REGION=us-east-1
```

## 🔄 How It Works

```
EventBridge Scheduler (every 1 min)
        ↓
    SQS Queue
        ↓
Fargate Worker (polls queue)
        ↓
Scans DynamoDB for VALID_STRUCTURE packages
        ↓
Processes validation (CSV, checksums, file comparison)
        ↓
Moves to Success (Asset Repo) or Error location
        ↓
Updates DynamoDB status
```

## 📋 Worker Processing Logic

For each package with `VALID_STRUCTURE` status:

1. ✅ **Check CSV Presence** → If missing, mark as `FAILED`
2. ✅ **Validate CSV Structure** → If invalid, mark as `INVALID_CSV`
3. ✅ **Process CSV Metadata** → Extract title and asset info
4. ✅ **Compare Files vs CSV** → Check for missing/extra files
5. ✅ **Validate Checksums** → Verify file integrity
6. ✅ **Move Files:**
   - Success → Asset Repository bucket
   - Failed → Error folder in Ingest bucket
7. ✅ **Update Status:** `SUCCESS`, `INVALID_CSV`, `MISSING_FILES`, `MISMATCH_CHECKSUM`, `EXTRA_FILES`
8. ✅ **Delete Source Files** → After successful verification

## 🧪 Test the Setup

### Send Test Message to SQS

```bash
aws sqs send-message \
  --queue-url YOUR_QUEUE_URL \
  --message-body '{"trigger": "asset_validation_check"}'
```

### Watch Worker Logs

```bash
aws logs tail /ecs/asset-validation-worker --follow
```

### Expected Log Output

```
Starting Asset Validation Worker...
Asset validation check triggered - scanning for VALID_STRUCTURE packages
Found 3 packages with VALID_STRUCTURE status
Processing package: 2590.0251/AssetID (created: 2025-01-15T10:30:00+00:00)
CSV present for 2590.0251/AssetID: True
Processing CSV: Upload/2590.0251/manifest.csv
Processed CSV metadata successfully for 2590.0251/AssetID
Successfully processed package: 2590.0251/AssetID
Validation check complete: 3 processed, 0 failed
```

## 🎯 Status Flow

```
PENDING → VALID_STRUCTURE → [Asset Validation Worker]
                                    ↓
            ┌───────────────────────┴────────────────────────┐
            ↓                                                 ↓
        SUCCESS                                           FAILED
    (Asset Repo)                                      (Error folder)
                                                           |
                        ┌──────────────────────────────────┴────────────┐
                        ↓                  ↓                ↓            ↓
                  INVALID_CSV    MISSING_FILES    EXTRA_FILES   MISMATCH_CHECKSUM
```

## 🔑 IAM Permissions Required

### Fargate Task Role

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "arn:aws:sqs:REGION:ACCOUNT:asset-validation-queue"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::routerunner-poc-ingest/*",
        "arn:aws:s3:::routerunner-poc-asset-repo/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:Query",
        "dynamodb:Scan",
        "dynamodb:GetItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem"
      ],
      "Resource": [
        "arn:aws:dynamodb:REGION:ACCOUNT:table/routerunner-poc-ingest-asset",
        "arn:aws:dynamodb:REGION:ACCOUNT:table/routerunner-poc-title-info",
        "arn:aws:dynamodb:REGION:ACCOUNT:table/routerunner-poc-asset-info",
        "arn:aws:dynamodb:REGION:ACCOUNT:table/*/index/*"
      ]
    }
  ]
}
```

### EventBridge Scheduler Role

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:REGION:ACCOUNT:asset-validation-queue"
    }
  ]
}
```

## 📊 Monitoring

### Key Metrics

- **SQS Queue Depth:** Should stay near 0 (worker processing quickly)
- **Message Age:** Should be < 1 minute
- **DynamoDB Scan Count:** Should match package count
- **Worker CPU/Memory:** Monitor for resource constraints

### CloudWatch Alarms

```bash
# Queue depth > 100 messages
# Old message age > 10 minutes
# Fargate task count < 1
```

## 🐛 Troubleshooting

| Issue | Solution |
|-------|----------|
| Worker not receiving messages | Check `AWS_SQS_ASSET_VALIDATION_QUEUE_URL` is correct |
| Messages not being deleted | Check IAM permissions for `sqs:DeleteMessage` |
| Validation not running | Check message format: `{"trigger": "asset_validation_check"}` |
| Files not moving | Check S3 bucket permissions |
| DynamoDB errors | Check table names and GSI configuration |

## 📝 Configuration Files Modified

- ✅ [config/settings.py](config/settings.py#L150) - Added `AWS_SQS_ASSET_VALIDATION_QUEUE_URL`
- ✅ [asset_validation_worker.py](da_processor/management/commands/asset_validation_worker.py) - SQS-based worker
- ✅ [docker-entrypoint.sh](docker-entrypoint.sh#L33-L35) - Service type configuration

## 🔄 Deployment Steps

1. **Update environment variables** in Fargate task definition
2. **Create SQS queue** for asset validation
3. **Configure EventBridge Scheduler** with message format above
4. **Deploy Fargate service** with `SERVICE_TYPE=ASSET_VALIDATION_WORKER`
5. **Test** with sample VALID_STRUCTURE package
6. **Monitor** logs and metrics
7. **Disable old Lambda** after verification

## ✅ Verification Checklist

- [ ] SQS queue created with correct name
- [ ] EventBridge Scheduler configured with `rate(1 minute)`
- [ ] Scheduler sends correct message format: `{"trigger": "asset_validation_check"}`
- [ ] Fargate task has correct environment variables
- [ ] IAM roles have required permissions
- [ ] Worker service is running and healthy
- [ ] Test message successfully processed
- [ ] CloudWatch logs show expected output
- [ ] DynamoDB status updates correctly
- [ ] Files move to correct locations (Success/Error)

---

**For detailed documentation, see:** [ASSET_VALIDATION_EVENTBRIDGE_CONFIG.md](ASSET_VALIDATION_EVENTBRIDGE_CONFIG.md)
