package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/oremj/dynamodb-autoscaler/autoscaler"
)

var tableName = flag.String("table", "", "The table to monitor.")

func main() {
	flag.Parse()
	if *tableName == "" {
		fmt.Println("-table must be set")
		os.Exit(1)
	}
	sess, err := session.NewSession(&aws.Config{Region: aws.String("us-east-1")})
	if err != nil {
		panic(err)
	}
	cw := autoscaler.NewDynamoTable(*tableName, sess)
	consumedRead, err := cw.ReadUnitsConsumed()
	if err != nil {
		panic(err)
	}
	fmt.Println(consumedRead / 5.0)
	consumedRead, err = cw.ReadUnitsProvisioned()
	if err != nil {
		panic(err)
	}
	fmt.Println(consumedRead)

	throttled, err := cw.WriteThrottledEvents()
	if err != nil {
		panic(err)
	}
	fmt.Println(throttled)
}
