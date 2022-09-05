package main

import (
	"fmt"
	"os"

	"github.com/akumor/elmercrawl/pkg/elmercrawl"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glue"
)

// TODO: don't set region via a constant
const region string = "us-west-2"

func main() {
	// TODO need to parse command line options
	awssession, err := session.NewSession(
		&aws.Config{
			Region: aws.String(region)},
	)
	if err != nil {
		fmt.Println("unable to create AWS session")
		os.Exit(1)
	}
	crawler := elmercrawl.Crawler{
		Glue: glue.New(awssession),
	}
	// TODO: done setup test catalog. use go tests instead
	fmt.Println("Setting up test glue catalog...")
	err = crawler.SetupTestGlueDataCatalog()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	fmt.Println("########")
	fmt.Println("Crawling databases...")
	crawler.CrawlDatabases(func(db *glue.Database) error {
		fmt.Println(*db.Name)
		return nil
	})
	fmt.Println("########")
	fmt.Println("Crawling tables...")
	crawler.CrawlTables(func(table *glue.TableData) error {
		fmt.Println(*table.Name)
		return nil
	})
	fmt.Println("########")
	fmt.Println("Crawling partitions...")
	crawler.CrawlPartitions(func(partition *glue.Partition) error {
		fmt.Printf("%v\n", partition)
		return nil
	})
}
