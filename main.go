package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/oremj/dynamodb-autoscaler/autoscaler"
)

var awsRegion = flag.String("region", "us-east-1", "The region where the table is located.")
var tableName = flag.String("table", "", "The table to monitor.")

func main() {
	flag.Parse()
	if *tableName == "" {
		fmt.Println("-table must be set")
		os.Exit(1)
	}

	sess, err := session.NewSession(&aws.Config{Region: aws.String(*awsRegion)})
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		dt := autoscaler.NewDynamoTable(*tableName, sess)
		dt.Monitor(5 * time.Second)
	}()
	wg.Wait()
}
