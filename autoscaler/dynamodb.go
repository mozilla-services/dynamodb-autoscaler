package autoscaler

import (
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var LookBackDuration = -5 * time.Minute

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

func (d *DynamoTable) checkTable() error {
	readConsumed, err := d.ReadUnitsConsumed()
	if err != nil {
		return fmt.Errorf("ReadUnitsConsumed: %v", err)
	}

	readProvisioned, err := d.ReadUnitsProvisioned()
	if err != nil {
		return fmt.Errorf("ReadUnitsProvisioned: %v", err)
	}

	writeConsumed, err := d.WriteUnitsConsumed()
	if err != nil {
		return fmt.Errorf("WriteUnitsConsumed: %v", err)
	}

	writeProvisioned, err := d.WriteUnitsProvisioned()
	if err != nil {
		return fmt.Errorf("WriteUnitsProvisioned: %v", err)
	}

	writeThrottled, err := d.WriteThrottledEvents()
	if err != nil {
		return fmt.Errorf("WriteThrottleEvents: %v", err)
	}

	readThrottled, err := d.ReadThrottledEvents()
	if err != nil {
		return fmt.Errorf("ReadThrottleEvents: %v", err)
	}

	fmt.Println(readConsumed, readProvisioned, writeConsumed, writeProvisioned, writeThrottled, readThrottled)

	return nil
}

func (d *DynamoTable) Monitor(freq time.Duration) {
	ticker := time.Tick(freq)
	for range ticker {
		// If the check fails to return after two intervals, panic
		timer := time.AfterFunc(2*freq, func() {
			panic("Deadlock detected.")
		})
		log.Println("Checking Table: ", d.TableName)
		if err := d.checkTable(); err != nil {
			log.Printf("checkTable: %v", err)
		}
		timer.Stop()
	}
}

func (d *DynamoTable) ReadUnitsConsumed() (int64, error) {
	return d.CloudWatch.ConsumedReadCapacityUnits(d.TableName, time.Now().Add(LookBackDuration))
}

func (d *DynamoTable) ReadUnitsProvisioned() (int64, error) {
	table, err := d.describeTable()
	if err != nil {
		return 0, err
	}

	return *table.ProvisionedThroughput.ReadCapacityUnits, nil
}

func (d *DynamoTable) WriteUnitsConsumed() (int64, error) {
	return d.CloudWatch.ConsumedWriteCapacityUnits(d.TableName, time.Now().Add(LookBackDuration))
}

func (d *DynamoTable) WriteUnitsProvisioned() (int64, error) {
	table, err := d.describeTable()
	if err != nil {
		return 0, err
	}

	return *table.ProvisionedThroughput.WriteCapacityUnits, nil
}

func (d *DynamoTable) ReadThrottledEvents() (int64, error) {
	return d.CloudWatch.ReadThrottleEvents(d.TableName, time.Now().Add(LookBackDuration))
}

func (d *DynamoTable) WriteThrottledEvents() (int64, error) {
	return d.CloudWatch.WriteThrottleEvents(d.TableName, time.Now().Add(LookBackDuration))
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
