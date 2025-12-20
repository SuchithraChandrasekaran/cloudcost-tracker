## Overview
This document defines the rules used to identify **wasteful cloud resources**.

## Waste Categories

### 1. Idle Resources
**Definition**: Resources with minimal activity that could be stopped or downsized.

**Detection Criteria**:
- **EC2 Instances**: CPU utilization < 5% for 7 consecutive days
- **RDS Instances**: Connection count < 1 per day for 7 days
- **Lambda Functions**: Zero invocations in last 30 days

**Waste Score Calculation**: 
```
Idle_Score = (Days_Idle / 7) * 100
```

### 2. Oversized Resources
**Definition**: Resources provisioned with capacity far exceeding actual usage.

**Detection Criteria**:
- **EC2 Instances**: Memory utilization < 30% average over 7 days
- **RDS Instances**: Storage utilization < 30%
- **Provisioned IOPS**: < 30% utilization

**Waste Score Calculation**:
```
Oversized_Score = (100 - Utilization_Percent) * Cost_Impact_Factor
```

### 3. Unused Resources
**Definition**: Resources not accessed within retention period.

**Detection Criteria**:
- **S3 Buckets**: No GET/PUT requests in last 30 days
- **EBS Volumes**: Unattached for > 30 days
- **Elastic IPs**: Not associated with running instances

**Waste Score Calculation**:
```
Unused_Score = Days_Unused * Daily_Cost
```

## Waste Severity Levels

| Waste Score | Category | Action Priority | Color Code |
|-------------|----------|-----------------|------------|
| > 1000      | Critical | Immediate       | Red        |
| 500-1000    | High     | Within 7 days   | Orange     |
| 100-500     | Medium   | Within 30 days  | Yellow     |
| < 100       | Low      | Monitor         | Green      |

## Monthly Waste Thresholds

| Monthly Waste | Category | Recommendation |
|---------------|----------|----------------|
| > $1,000      | Critical | Stop/terminate immediately |
| $500-$1,000   | High     | Rightsize or stop |
| < $500        | Medium   | Schedule review |

## Implementation Notes

- All metrics are calculated using a 7-day rolling window
- Waste scores are cumulative across all waste types
- Resources can have multiple waste flags simultaneously
- Cost calculations use unblended costs from AWS Cost Explorer
