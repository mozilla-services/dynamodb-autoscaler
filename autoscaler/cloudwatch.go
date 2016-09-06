package autoscaler

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

type CloudWatch struct {
	Service           *cloudwatch.CloudWatch
	EvaluationMinutes int64
}

func NewCloudWatch(awsSession *session.Session) *CloudWatch {
	return &CloudWatch{
		Service:           cloudwatch.New(awsSession),
		EvaluationMinutes: 5,
	}
}

func (c *CloudWatch) sumMetric(metricName, tableName string, startTime time.Time) (float64, error) {
	endTime := startTime.Add(time.Duration(c.EvaluationMinutes) * time.Minute)
	input := &cloudwatch.GetMetricStatisticsInput{
		Dimensions: []*cloudwatch.Dimension{
			&cloudwatch.Dimension{
				Name:  aws.String("TableName"),
				Value: aws.String(tableName),
			},
		},
		EndTime:    aws.Time(endTime),
		MetricName: aws.String(metricName),
		Namespace:  aws.String("AWS/DynamoDB"),
		Period:     aws.Int64(c.EvaluationMinutes * 60),
		StartTime:  aws.Time(startTime),
		Statistics: []*string{
			aws.String("Sum"),
		},
	}

	out, err := c.Service.GetMetricStatistics(input)
	if err != nil {
		return 0, fmt.Errorf("GetMetricStatistics: %v", err)
	}

	if len(out.Datapoints) < 1 {
		return 0, nil
	}

	return *out.Datapoints[0].Sum, nil
}

func (c *CloudWatch) ReadThrottleEvents(tableName string, startTime time.Time) (float64, error) {
	return c.sumMetric("ReadThrottleEvents", tableName, startTime)
}

func (c *CloudWatch) WriteThrottleEvents(tableName string, startTime time.Time) (float64, error) {
	return c.sumMetric("WriteThrottleEvents", tableName, startTime)
}

func (c *CloudWatch) ConsumedReadCapacityUnits(tableName string, startTime time.Time) (float64, error) {
	return c.sumMetric("ConsumedReadCapacityUnits", tableName, startTime)
}

func (c *CloudWatch) ConsumedWriteCapacityUnits(tableName string, startTime time.Time) (float64, error) {
	return c.sumMetric("ConsumedWriteCapacityUnits", tableName, startTime)
}
