package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"text/template"

	"github.com/akumor/elmercrawl/pkg/elmercrawl"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/spf13/cobra"
)

const version string = "0.0.1"

type RootOpts struct {
	AWSRegion string
	CatalogId string
}

func main() {

	rootOpts := RootOpts{}

	rootCmd := &cobra.Command{
		Use:     "elmercrawl",
		Short:   "Perform operations against resources in an AWS glue data catalog",
		Version: version,
	}

	rootCmd.Flags().StringVarP(&rootOpts.AWSRegion, "aws-region", "p", "us-east-1", "AWS region for the glue data catalog")
	rootCmd.Flags().StringVarP(&rootOpts.CatalogId, "catalog-id", "C", "", "ID of the AWS Glue Data Catalog to target")

	databasesCmd := &cobra.Command{
		Use:   "databases [command]",
		Short: "Run some command against every database in the specified AWS glue data catalog",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			command := ""
			if len(args) != 0 {
				command = args[0]
			}
			crawler, err := getCrawler(rootOpts.AWSRegion, rootOpts.CatalogId)
			if err != nil {
				return fmt.Errorf("unable to create crawler: %w", err)
			}
			fmt.Println("Crawling databases...")
			err = crawler.CrawlDatabases(func(db *glue.Database) error {
				if command == "" {
					fmt.Println(*db.Name)
				} else {
					tmpl, err := template.New("databases").Parse(command)
					if err != nil {
						return fmt.Errorf("failed to parse databases command template: %w", err)
					}
					buf := new(bytes.Buffer)
					err = tmpl.Execute(buf, *db)
					if err != nil {
						return fmt.Errorf("failed to render databases command template: %w", err)
					}
					cmd := exec.Command("bash", "-c", buf.String())
					var stdout bytes.Buffer
					var stderr bytes.Buffer
					cmd.Stdout = &stdout
					cmd.Stderr = &stderr
					err = cmd.Run()
					if err != nil {
						return fmt.Errorf("failed to run databases function: %w", err)
					}
					fmt.Println("--- stdout ---")
					fmt.Println(stdout.String())
					fmt.Println("--- stderr ---")
					fmt.Println(stderr.String())
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to crawl databases: %w", err)
			}
			return nil
		},
	}

	rootCmd.AddCommand(databasesCmd)

	tablesCmd := &cobra.Command{
		Use:   "tables [command]",
		Short: "Run some command against every table in the specified AWS glue data catalog",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			command := ""
			if len(args) != 0 {
				command = args[0]
			}
			crawler, err := getCrawler(rootOpts.AWSRegion, rootOpts.CatalogId)
			if err != nil {
				return fmt.Errorf("unable to create crawler: %w", err)
			}
			fmt.Println("Crawling tables...")
			err = crawler.CrawlTables(func(table *glue.TableData) error {
				if command == "" {
					fmt.Println(*table.Name)
				} else {
					tmpl, err := template.New("tables").Parse(command)
					if err != nil {
						return fmt.Errorf("failed to parse tables command: %w", err)
					}
					buf := new(bytes.Buffer)
					err = tmpl.Execute(buf, *table)
					if err != nil {
						return fmt.Errorf("failed to render tables command: %w", err)
					}
					cmd := exec.Command("bash", "-c", buf.String())
					var stdout bytes.Buffer
					var stderr bytes.Buffer
					cmd.Stdout = &stdout
					cmd.Stderr = &stderr
					err = cmd.Run()
					if err != nil {
						return fmt.Errorf("failed to run tables function: %w", err)
					}
					fmt.Println("--- stdout ---")
					fmt.Println(stdout.String())
					fmt.Println("--- stderr ---")
					fmt.Println(stderr.String())
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to crawl tables: %w", err)
			}
			return nil
		},
	}

	rootCmd.AddCommand(tablesCmd)

	partitionsCmd := &cobra.Command{
		Use:   "partitions [command]",
		Short: "Run some command against every partition in the specified AWS glue data catalog",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			command := ""
			if len(args) != 0 {
				command = args[0]
			}
			crawler, err := getCrawler(rootOpts.AWSRegion, rootOpts.CatalogId)
			if err != nil {
				return fmt.Errorf("unable to create crawler: %w", err)
			}
			fmt.Println("Crawling partitions...")
			err = crawler.CrawlPartitions(func(partition *glue.Partition) error {
				if command == "" {
					fmt.Printf("%v\n", partition)
				} else {
					tmpl, err := template.New("partitions").Parse(command)
					if err != nil {
						return fmt.Errorf("failed to parse partitions command template: %w", err)
					}
					buf := new(bytes.Buffer)
					err = tmpl.Execute(buf, *partition)
					if err != nil {
						return fmt.Errorf("failed to render partitions command: %w", err)
					}
					cmd := exec.Command("bash", "-c", buf.String())
					var stdout bytes.Buffer
					var stderr bytes.Buffer
					cmd.Stdout = &stdout
					cmd.Stderr = &stderr
					err = cmd.Run()
					if err != nil {
						return fmt.Errorf("failed to run partitions function: %w", err)
					}
					fmt.Println("--- stdout ---")
					fmt.Println(stdout.String())
					fmt.Println("--- stderr ---")
					fmt.Println(stderr.String())
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to crawl partitions: %w", err)
			}
			return nil
		},
	}

	rootCmd.AddCommand(partitionsCmd)

	testCatalogCmd := &cobra.Command{
		Use:   "testcatalog",
		Short: "Create a glue database, table, and partition in the specified AWS glue data catalog for testing",
		Args:  cobra.MaximumNArgs(0),
		RunE: func(_ *cobra.Command, args []string) error {
			crawler, err := getCrawler(rootOpts.AWSRegion, rootOpts.CatalogId)
			if err != nil {
				return fmt.Errorf("unable to create crawler: %w", err)
			}
			fmt.Println("Setting up test glue catalog...")
			err = crawler.SetupTestGlueDataCatalog()
			if err != nil {
				return fmt.Errorf("testcatalog subcommand failed: %w", err)
			}
			return nil
		},
	}

	rootCmd.AddCommand(testCatalogCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %v", err)
		os.Exit(1)
	}
}

func getCrawler(region, catalogId string) (elmercrawl.Crawler, error) {
	sess, err := session.NewSession(
		&aws.Config{
			Region: aws.String(region),
		},
	)
	if err != nil {
		return elmercrawl.Crawler{}, fmt.Errorf("unable to create AWS session: %w", err)
	}
	crawler := elmercrawl.Crawler{
		Glue:      glue.New(sess),
		CatalogId: catalogId,
	}
	return crawler, nil
}
