package autoscaler

import (
	"fmt"
	"log"
	"sync"
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

	Cooldown time.Duration

	readThreshold float64
	writeTheshold float64

	writeThrottledThreshold float64
	readThrottledThreshold  float64
}

func NewDynamoTable(tableName string, awsSession *session.Session) *DynamoTable {
	return &DynamoTable{
		TableName:  tableName,
		CloudWatch: NewCloudWatch(awsSession),
		Service:    dynamodb.New(awsSession),
	}
}

func (d *DynamoTable) checkReads() (bool, error) {
	readConsumed, err := d.ReadUnitsConsumed()
	if err != nil {
		return false, fmt.Errorf("ReadUnitsConsumed: %v", err)
	}

	readProvisioned, err := d.ReadUnitsProvisioned()
	if err != nil {
		return false, fmt.Errorf("ReadUnitsProvisioned: %v", err)
	}

	readThrottled, err := d.ReadThrottledEvents()
	if err != nil {
		return false, fmt.Errorf("ReadThrottleEvents: %v", err)
	}

	if readConsumed > float64(readProvisioned)*(1-d.readThreshold) {
		return true, nil
	}
	if readThrottled > d.readThrottledThreshold {
		return true, nil
	}

	return false, nil
}

func (d *DynamoTable) checkWrites() (bool, error) {
	writeConsumed, err := d.WriteUnitsConsumed()
	if err != nil {
		return false, fmt.Errorf("WriteUnitsConsumed: %v", err)
	}

	writeProvisioned, err := d.WriteUnitsProvisioned()
	if err != nil {
		return false, fmt.Errorf("WriteUnitsProvisioned: %v", err)
	}

	writeThrottled, err := d.WriteThrottledEvents()
	if err != nil {
		return false, fmt.Errorf("WriteThrottleEvents: %v", err)
	}

	if writeConsumed > float64(writeProvisioned)*(1-d.writeTheshold) {
		return true, nil
	}
	if writeThrottled > d.writeThrottledThreshold {
		return true, nil
	}

	return false, nil
}

func (d *DynamoTable) monitor(freq time.Duration, monitorFuncName string, monitorFunc func() (bool, error)) {
	ticker := time.Tick(freq)
	for range ticker {
		// If the check fails to return after two intervals, panic
		timer := time.AfterFunc(2*freq, func() {
			panic(monitorFuncName + " deadlock detected.")
		})

		log.Printf("Checking %s: %s", monitorFuncName, d.TableName)
		scaled, err := monitorFunc()
		if err != nil {
			log.Printf("%s: %v", monitorFuncName, err)
		}

		timer.Stop()
		if scaled {
			time.Sleep(d.Cooldown)
		}
	}

}

func (d *DynamoTable) Monitor(freq time.Duration) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		d.monitor(freq, "checkReads", d.checkReads)
	}()
	go func() {
		defer wg.Done()
		d.monitor(freq, "checkWrites", d.checkWrites)
	}()
	wg.Wait()
}

func (d *DynamoTable) ReadUnitsConsumed() (float64, error) {
	return d.CloudWatch.ConsumedReadCapacityUnits(d.TableName, time.Now().Add(LookBackDuration))
}

func (d *DynamoTable) ReadUnitsProvisioned() (int64, error) {
	table, err := d.describeTable()
	if err != nil {
		return 0, err
	}

	return *table.ProvisionedThroughput.ReadCapacityUnits, nil
}

func (d *DynamoTable) WriteUnitsConsumed() (float64, error) {
	return d.CloudWatch.ConsumedWriteCapacityUnits(d.TableName, time.Now().Add(LookBackDuration))
}

func (d *DynamoTable) WriteUnitsProvisioned() (int64, error) {
	table, err := d.describeTable()
	if err != nil {
		return 0, err
	}

	return *table.ProvisionedThroughput.WriteCapacityUnits, nil
}

func (d *DynamoTable) ReadThrottledEvents() (float64, error) {
	return d.CloudWatch.ReadThrottleEvents(d.TableName, time.Now().Add(LookBackDuration))
}

func (d *DynamoTable) WriteThrottledEvents() (float64, error) {
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
