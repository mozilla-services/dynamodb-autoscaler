package autoscaler

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DynamoTable struct {
	TableName string

	CloudWatch *CloudWatch
	Service    *dynamodb.DynamoDB
}

func NewDynamoTable(tableName string, awsSession *session.Session) *DynamoTable {
	return &DynamoTable{
		TableName:  tableName,
		CloudWatch: NewCloudWatch(awsSession),
		Service:    dynamodb.New(awsSession),
	}
}

func (d *DynamoTable) ReadUnitsConsumed() (int64, error) {
	return d.CloudWatch.ConsumedReadCapacityUnits(d.TableName, time.Now().Add(-20*time.Minute))
}

func (d *DynamoTable) ReadUnitsProvisioned() (int64, error) {
	table, err := d.describeTable()
	if err != nil {
		return 0, err
	}

	return *table.ProvisionedThroughput.ReadCapacityUnits, nil
}

func (d *DynamoTable) WriteUnitsConsumed() (int64, error) {
	return d.CloudWatch.ConsumedWriteCapacityUnits(d.TableName, time.Now().Add(-20*time.Minute))
}

func (d *DynamoTable) WriteUnitsProvisioned() (int64, error) {
	table, err := d.describeTable()
	if err != nil {
		return 0, err
	}

	return *table.ProvisionedThroughput.WriteCapacityUnits, nil
}

func (d *DynamoTable) ReadThrottledEvents() (int64, error) {
	return d.CloudWatch.ReadThrottleEvents(d.TableName, time.Now().Add(-20*time.Minute))
}

func (d *DynamoTable) WriteThrottledEvents() (int64, error) {
	return d.CloudWatch.WriteThrottleEvents(d.TableName, time.Now().Add(-20*time.Minute))
}

func (d *DynamoTable) describeTable() (*dynamodb.TableDescription, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(d.TableName),
	}
	out, err := d.Service.DescribeTable(input)
	if err != nil {
		return nil, fmt.Errorf("DescribeTable: %v", err)
	}

	return out.Table, nil
}
