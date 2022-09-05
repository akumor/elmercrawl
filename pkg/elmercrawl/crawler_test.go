package elmercrawl

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/aws/aws-sdk-go/service/glue/glueiface"
)

type mockedGetDatabases struct {
	glueiface.GlueAPI
	Resp    glue.GetDatabasesOutput
	RespTwo glue.GetDatabasesOutput
}

func (m mockedGetDatabases) GetDatabases(in *glue.GetDatabasesInput) (*glue.GetDatabasesOutput, error) {
	// Only need to return mocked response output
	if in.NextToken == nil {
		return &m.Resp, nil
	} else {
		return &m.RespTwo, nil
	}
}

func TestCrawlDatabases(t *testing.T) {
	cases := []struct {
		Resp     glue.GetDatabasesOutput
		RespTwo  glue.GetDatabasesOutput
		Expected []*glue.Database
		GDBF     glueDBFunc
	}{
		{
			Resp: glue.GetDatabasesOutput{
				DatabaseList: []*glue.Database{
					{
						Name: aws.String("testdb"),
					},
				},
				NextToken: nil,
			},
			RespTwo: glue.GetDatabasesOutput{},
			Expected: []*glue.Database{
				{
					Name: aws.String("testdb"),
				},
			},
			GDBF: func(d *glue.Database) error { return nil },
		},
		{
			Resp: glue.GetDatabasesOutput{
				DatabaseList: []*glue.Database{
					{
						Name: aws.String("testdb"),
					},
					{
						Name: aws.String("testdb2"),
					},
				},
				NextToken: aws.String("mocktoken"),
			},
			RespTwo: glue.GetDatabasesOutput{
				DatabaseList: []*glue.Database{
					{
						Name: aws.String("testdb3"),
					},
					{
						Name: aws.String("testdb4"),
					},
				},
				NextToken: nil,
			},
			Expected: []*glue.Database{
				{
					Name: aws.String("testdb"),
				},
				{
					Name: aws.String("testdb2"),
				},
				{
					Name: aws.String("testdb3"),
				},
				{
					Name: aws.String("testdb4"),
				},
			},
			GDBF: func(d *glue.Database) error { return nil },
		},
	}

	for i, c := range cases {
		crawler := Crawler{Glue: mockedGetDatabases{Resp: c.Resp, RespTwo: c.RespTwo}}
		err := crawler.CrawlDatabases(c.GDBF)
		if err != nil {
			t.Fatalf("%d, unexpected error", err)
		}
		if len(crawler.databases) != len(c.Expected) {
			t.Fatalf("%d, expected %d databases, got %d", i, len(c.Expected), len(crawler.databases))
		}
		for i := range c.Expected {
			if *c.Expected[i].Name != *crawler.databases[i].Name {
				t.Fatalf("%d, expected %s database name, got %s", i, *c.Expected[i].Name, *crawler.databases[i].Name)
			}
		}
	}
}

type mockedGetTables struct {
	glueiface.GlueAPI
	Resp    glue.GetTablesOutput
	RespTwo glue.GetTablesOutput
}

func (m mockedGetTables) GetTables(in *glue.GetTablesInput) (*glue.GetTablesOutput, error) {
	// Only need to return mocked response output
	if in.NextToken == nil {
		return &m.Resp, nil
	} else {
		return &m.RespTwo, nil
	}
}

func TestCrawlTables(t *testing.T) {
	cases := []struct {
		Databases []*glue.Database
		Resp      glue.GetTablesOutput
		RespTwo   glue.GetTablesOutput
		Expected  []*glue.TableData
		GTF       glueTableFunc
	}{
		{
			Databases: []*glue.Database{
				{
					Name: aws.String("testdb"),
				},
			},
			Resp: glue.GetTablesOutput{
				TableList: []*glue.TableData{
					{
						Name: aws.String("testtable"),
					},
				},
				NextToken: nil,
			},
			RespTwo: glue.GetTablesOutput{},
			Expected: []*glue.TableData{
				{
					Name: aws.String("testtable"),
				},
			},
			GTF: func(t *glue.TableData) error { return nil },
		},
		{
			Databases: []*glue.Database{
				{
					Name: aws.String("testdb"),
				},
			},
			Resp: glue.GetTablesOutput{
				TableList: []*glue.TableData{
					{
						Name: aws.String("testtable"),
					},
					{
						Name: aws.String("testtable2"),
					},
				},
				NextToken: aws.String("mocktoken"),
			},
			RespTwo: glue.GetTablesOutput{
				TableList: []*glue.TableData{
					{
						Name: aws.String("testtable3"),
					},
					{
						Name: aws.String("testtable4"),
					},
				},
			},
			Expected: []*glue.TableData{
				{
					Name: aws.String("testtable"),
				},
				{
					Name: aws.String("testtable2"),
				},
				{
					Name: aws.String("testtable3"),
				},
				{
					Name: aws.String("testtable4"),
				},
			},
			GTF: func(t *glue.TableData) error { return nil },
		},
	}

	for i, c := range cases {
		crawler := Crawler{
			Glue: mockedGetTables{
				Resp:    c.Resp,
				RespTwo: c.RespTwo,
			},
			databases: c.Databases,
		}
		err := crawler.CrawlTables(c.GTF)
		if err != nil {
			t.Fatalf("%d, unexpected error", err)
		}
		if len(crawler.tables) != len(c.Expected) {
			t.Fatalf("%d, expected %d tables, got %d", i, len(c.Expected), len(crawler.tables))
		}
		for i := range c.Expected {
			if *c.Expected[i].Name != *crawler.tables[i].Name {
				t.Fatalf("%d, expected %s table name, got %s", i, *c.Expected[i].Name, *crawler.tables[i].Name)
			}
		}
	}
}

type mockedGetPartitions struct {
	glueiface.GlueAPI
	Resp    glue.GetPartitionsOutput
	RespTwo glue.GetPartitionsOutput
}

func (m mockedGetPartitions) GetPartitions(in *glue.GetPartitionsInput) (*glue.GetPartitionsOutput, error) {
	// Only need to return mocked response output
	if in.NextToken == nil {
		return &m.Resp, nil
	} else {
		return &m.RespTwo, nil
	}
}

func TestCrawlPartitions(t *testing.T) {
	cases := []struct {
		Databases []*glue.Database
		Tables    []*glue.TableData
		Resp      glue.GetPartitionsOutput
		RespTwo   glue.GetPartitionsOutput
		Expected  []*glue.Partition
		GPF       gluePartitionFunc
	}{
		{
			Databases: []*glue.Database{
				{
					Name: aws.String("testdb"),
				},
			},
			Tables: []*glue.TableData{
				{
					Name: aws.String("testtable"),
				},
			},
			Resp: glue.GetPartitionsOutput{
				Partitions: []*glue.Partition{
					{
						DatabaseName: aws.String("testdb"),
						TableName:    aws.String("testtable"),
						Values: []*string{
							aws.String("20220903"),
						},
					},
				},
				NextToken: nil,
			},
			RespTwo: glue.GetPartitionsOutput{},
			Expected: []*glue.Partition{
				{
					DatabaseName: aws.String("testdb"),
					TableName:    aws.String("testtable"),
					Values: []*string{
						aws.String("20220903"),
					},
				},
			},
			GPF: func(p *glue.Partition) error { return nil },
		},
		{
			Databases: []*glue.Database{
				{
					Name: aws.String("testdb"),
				},
				{
					Name: aws.String("testdb2"),
				},
			},
			Tables: []*glue.TableData{
				{
					Name: aws.String("testtable"),
				},
				{
					Name: aws.String("testtable2"),
				},
			},
			Resp: glue.GetPartitionsOutput{
				Partitions: []*glue.Partition{
					{
						DatabaseName: aws.String("testdb"),
						TableName:    aws.String("testtable"),
						Values: []*string{
							aws.String("20220903"),
						},
					},
					{
						DatabaseName: aws.String("testdb2"),
						TableName:    aws.String("testtable2"),
						Values: []*string{
							aws.String("20220904"),
						},
					},
				},
				NextToken: aws.String("mocktoken"),
			},
			RespTwo: glue.GetPartitionsOutput{
				Partitions: []*glue.Partition{
					{
						DatabaseName: aws.String("testdb"),
						TableName:    aws.String("testtable"),
						Values: []*string{
							aws.String("20220905"),
						},
					},
					{
						DatabaseName: aws.String("testdb2"),
						TableName:    aws.String("testtable2"),
						Values: []*string{
							aws.String("20220906"),
						},
					},
				},
				NextToken: nil,
			},
			Expected: []*glue.Partition{
				{
					DatabaseName: aws.String("testdb"),
					TableName:    aws.String("testtable"),
					Values: []*string{
						aws.String("20220903"),
					},
				},
				{
					DatabaseName: aws.String("testdb2"),
					TableName:    aws.String("testtable2"),
					Values: []*string{
						aws.String("20220904"),
					},
				},
				{
					DatabaseName: aws.String("testdb"),
					TableName:    aws.String("testtable"),
					Values: []*string{
						aws.String("20220905"),
					},
				},
				{
					DatabaseName: aws.String("testdb2"),
					TableName:    aws.String("testtable2"),
					Values: []*string{
						aws.String("20220906"),
					},
				},
			},
			GPF: func(p *glue.Partition) error { return nil },
		},
	}

	for i, c := range cases {
		crawler := Crawler{
			Glue: mockedGetPartitions{
				Resp:    c.Resp,
				RespTwo: c.RespTwo,
			},
			databases: c.Databases,
			tables:    c.Tables,
		}
		err := crawler.CrawlPartitions(c.GPF)
		if err != nil {
			t.Fatalf("%d, unexpected error", err)
		}
		if len(crawler.partitions) != len(c.Expected) {
			t.Fatalf("%d, expected %d partitions, got %d", i, len(c.Expected), len(crawler.partitions))
		}
		for i := range c.Expected {
			if *c.Expected[i].DatabaseName != *crawler.partitions[i].DatabaseName {
				t.Fatalf("%d, expected %s database name, got %s", i, *c.Expected[i].DatabaseName, *crawler.partitions[i].DatabaseName)
			}
			if *c.Expected[i].TableName != *crawler.partitions[i].TableName {
				t.Fatalf("%d, expected %s table name, got %s", i, *c.Expected[i].TableName, *crawler.partitions[i].TableName)
			}
			for j := range c.Expected[i].Values {
				if *c.Expected[i].Values[j] != *crawler.partitions[i].Values[j] {
					t.Fatalf("%d, expected %s value, got %s", i, *c.Expected[i].Values[j], *crawler.partitions[i].Values[j])
				}
			}
		}
	}
}
