package elmercrawl

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/aws/aws-sdk-go/service/glue/glueiface"
)

type Crawler struct {
	Glue       glueiface.GlueAPI
	CatalogId  string
	databases  []*glue.Database
	tables     []*glue.TableData
	partitions []*glue.Partition
}

type glueDBFunc func(*glue.Database) error
type glueTableFunc func(*glue.TableData) error
type gluePartitionFunc func(*glue.Partition) error

func (c *Crawler) CrawlDatabases(gdbf glueDBFunc) error {
	if c.databases == nil {
		err := c.getDatabases()
		if err != nil {
			return fmt.Errorf("CrawlDatabases failed to get databases: %w", err)
		}
	}
	for i := range c.databases {
		err := gdbf(c.databases[i])
		if err != nil {
			return fmt.Errorf("CrawlDatabases failed to run function: %w", err)
		}
	}
	return nil
}

func (c *Crawler) getDatabases() error {
	getDbOut, err := c.Glue.GetDatabases(&glue.GetDatabasesInput{})
	if err != nil {
		return fmt.Errorf("getDatabases failed to get databases: %w", err)
	}
	c.databases = getDbOut.DatabaseList
	for {
		if getDbOut.NextToken == nil {
			break
		}
		getDbOut, err = c.Glue.GetDatabases(&glue.GetDatabasesInput{
			NextToken: getDbOut.NextToken,
		})
		if err != nil {
			return fmt.Errorf("getDatabases failed to get databases with token: %w", err)
		}
		c.databases = append(c.databases, getDbOut.DatabaseList...)
	}
	return nil
}

func (c *Crawler) CrawlTables(gtf glueTableFunc) error {
	if c.tables == nil {
		err := c.getTables()
		if err != nil {
			return fmt.Errorf("CrawlTables failed to get tables: %w", err)
		}
	}
	for i := range c.tables {
		err := gtf(c.tables[i])
		if err != nil {
			return fmt.Errorf("CrawlTables failed to run function: %w", err)
		}
	}
	return nil
}

func (c *Crawler) getTables() error {
	if c.databases == nil {
		err := c.getDatabases()
		if err != nil {
			return fmt.Errorf("getTables failed to get databases: %w", err)
		}
	}
	for i := range c.databases {
		getTblOut, err := c.Glue.GetTables(&glue.GetTablesInput{
			DatabaseName: c.databases[i].Name,
		})
		if err != nil {
			return fmt.Errorf("getTables failed to get tables: %w", err)
		}
		if c.tables == nil {
			c.tables = getTblOut.TableList
		} else {
			c.tables = append(c.tables, getTblOut.TableList...)
		}
		for {
			if getTblOut.NextToken == nil {
				break
			}
			getTblOut, err = c.Glue.GetTables(&glue.GetTablesInput{
				DatabaseName: c.databases[i].Name,
				NextToken:    getTblOut.NextToken,
			})
			if err != nil {
				return fmt.Errorf("getTables failed to get tables with token: %w", err)
			}
			c.tables = append(c.tables, getTblOut.TableList...)
		}
	}
	return nil
}

func (c *Crawler) CrawlPartitions(gpf gluePartitionFunc) error {
	if c.partitions == nil {
		err := c.getPartitions()
		if err != nil {
			return fmt.Errorf("CrawlPartitions failed to get partitions: %w", err)
		}
	}
	for i := range c.partitions {
		err := gpf(c.partitions[i])
		if err != nil {
			return fmt.Errorf("CrawlPartitions failed to run function: %w", err)
		}
	}
	return nil
}

func (c *Crawler) getPartitions() error {
	if c.tables == nil {
		err := c.getTables()
		if err != nil {
			return fmt.Errorf("getPartitions failed to get tables: %w", err)
		}
	}
	for i := range c.tables {
		getPartOut, err := c.Glue.GetPartitions(&glue.GetPartitionsInput{
			DatabaseName: c.tables[i].DatabaseName,
			TableName:    c.tables[i].Name,
		})
		if err != nil {
			return fmt.Errorf("getPartitions failed to get partitions: %w", err)
		}
		if c.partitions == nil {
			c.partitions = getPartOut.Partitions
		} else {
			c.partitions = append(c.partitions, getPartOut.Partitions...)
		}
		for {
			if getPartOut.NextToken == nil {
				break
			}
			getPartOut, err = c.Glue.GetPartitions(&glue.GetPartitionsInput{
				DatabaseName: c.tables[i].DatabaseName,
				TableName:    c.tables[i].Name,
				NextToken:    getPartOut.NextToken,
			})
			if err != nil {
				return fmt.Errorf("getPartitions failed to get partitions with token: %w", err)
			}
			c.partitions = append(c.partitions, getPartOut.Partitions...)
		}
	}
	return nil
}

func (c *Crawler) SetupTestGlueDataCatalog() error {
	_, err := c.Glue.CreateDatabase(&glue.CreateDatabaseInput{
		DatabaseInput: &glue.DatabaseInput{
			Name: aws.String("testdb"),
		},
	})
	if err != nil {
		if !strings.HasPrefix(err.Error(), "AlreadyExistsException") {
			return err
		}
	}
	_, err = c.Glue.CreateTable(&glue.CreateTableInput{
		DatabaseName: aws.String("testdb"),
		TableInput: &glue.TableInput{
			Name: aws.String("testtable"),
			StorageDescriptor: &glue.StorageDescriptor{
				Columns: []*glue.Column{
					{
						Name: aws.String("logdate"),
						Type: aws.String("int"),
					},
				},
				Location: aws.String("s3://bucket-path/"),
				SerdeInfo: &glue.SerDeInfo{
					SerializationLibrary: aws.String("org.openx.data.jsonserde.JsonSerDe"),
				},
			},
			Parameters: map[string]*string{
				"classification": aws.String("json"),
			},
			PartitionKeys: []*glue.Column{
				{
					Name: aws.String("logdate"),
					Type: aws.String("int"),
				},
			},
		},
	})
	if err != nil {
		if !strings.HasPrefix(err.Error(), "AlreadyExistsException") {
			return err
		}
	}
	_, err = c.Glue.CreatePartition(&glue.CreatePartitionInput{
		DatabaseName: aws.String("testdb"),
		TableName:    aws.String("testtable"),
		PartitionInput: &glue.PartitionInput{
			Values: []*string{
				aws.String("20220902"),
			},
			StorageDescriptor: &glue.StorageDescriptor{
				Columns: []*glue.Column{
					{
						Name: aws.String("logdate"),
						Type: aws.String("int"),
					},
				},
			},
		},
	})
	if err != nil {
		if !strings.HasPrefix(err.Error(), "AlreadyExistsException") {
			return err
		}
	}
	return nil
}
