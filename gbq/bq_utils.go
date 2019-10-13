package bqtools

import (
	"os"
	"fmt"
	"time"
	"reflect"
	
	"github.com/gammazero/workerpool"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"net/http"

	bu "github.com/belboo/boo-go-tools/misc"
)

// SchemasMatch checks schemas field by field (by name and type) to see if they match
func SchemasMatch(s1 bigquery.Schema, s2 bigquery.Schema) bool {
	for i := range s1 {
		if s1[i].Name != s2[i].Name || s1[i].Type != s2[i].Type {
			return false
		}
	}
	return true
}

// HasField checks if schema has a given field
func HasField(schema *bigquery.Schema, field string) bool {
	if len(*schema) == 0 {
		return false
	}

	for _, f := range *schema {
		if f.Name == field {
			return true
		}
	}

	return false
}

// GetDatasetMetaCopy is a semi-deep-copy of bigquery.DatasetMetadata object
func GetDatasetMetaCopy(from *bigquery.DatasetMetadata) *bigquery.DatasetMetadata {
	newDatasetMeta := &bigquery.DatasetMetadata{
		Name:                   from.Name,
		Labels:                 from.Labels,
		Description:            from.Description,
		DefaultTableExpiration: from.DefaultTableExpiration,
		Location:               from.Location,
	}

	return newDatasetMeta
}

// GetTableMetaCopy is a semi-deep-copy of bigquery.TableMetadata object
func GetTableMetaCopy(from *bigquery.TableMetadata) *bigquery.TableMetadata {
	newTableMeta := &bigquery.TableMetadata{
		Name:             from.Name,
		Schema:           from.Schema,
		Description:      from.Description,
		EncryptionConfig: from.EncryptionConfig,
		ExpirationTime:   from.ExpirationTime,
		Labels:           from.Labels,
		TimePartitioning: from.TimePartitioning,
	}

	return newTableMeta
}

// GetBQClient returns a bigquery client
// TODO: Refactor after config and CLI implementation
func GetBQClient(ctx context.Context, project string) (*bigquery.Client, error) {
	if project == "" {
		project = os.Getenv("GOOGLE_CLOUD_PROJECT")
	}
	if project == "" {
		return nil, bu.TError{}.WithMsg("project has to be specified or GOOGLE_CLOUD_PROJECT environment variable must be set")
	}

	client, err := bigquery.NewClient(ctx, project)
	if err != nil {
		return nil, bu.TError{}.WithMsg("failed to create BigQuery client...")
	}

	return client, nil
}

// CreateBQTable is a thin envelope for BQ table creation
func CreateBQTable(ctx context.Context, bqClient *bigquery.Client, 
	bqDataset string, bqTable string, bqSchema bigquery.Schema, 
	bqTableDescription string, bqTableLabels map[string]string, 
	bqTimePartitionField string, bqTableExpiration int, delete bool) error {

	var err error

	dstDS := bqClient.Dataset(bqDataset)
	dstT := dstDS.Table(bqTable)
	_, err = dstT.Metadata(ctx)
	if err == nil {
		if delete {
			err = dstT.Delete(ctx)
			if err != nil {
				return bu.TError{
					Msg:    fmt.Sprintf("failed to delete bqTable %v.%v", bqDataset, bqTable),
					Origin: "CreateBQTable",
					Code:   bu.ErrDeleteTable,
					Err:    err,
				}
			}
		} else {
			return nil
		}
	}
	// Check if meta fetch failed for some other reason than notFound
	if erg, ok := err.(*googleapi.Error); ok {
		if erg.Code != http.StatusNotFound || (len(erg.Errors) > 0 && erg.Errors[0].Reason != "notFound") {
			return bu.TError{
				Msg:    fmt.Sprintf("something went wrong while trying to fetch/(re)create bqTable %v.%v", bqDataset, bqTable),
				Origin: "CreateBQTable",
				Code:   bu.ErrGetTableMeta,
				Err:    err,
			}
		}
	}

	timePartitioning := &bigquery.TimePartitioning{Expiration: 0}

	if len(bqTimePartitionField) > 0 {
		timePartitioning.Field = bqTimePartitionField
	}

	dstTMeta := &bigquery.TableMetadata{
		Name:             bqTable,
		Schema:           bqSchema,
		Description:      bqTableDescription,
		Labels:           bqTableLabels,
		TimePartitioning: timePartitioning,
	}

	err = dstT.Create(ctx, dstTMeta)
	if err != nil {
		return bu.TError{
			Msg:    fmt.Sprintf("failed to create bqTable %v.%v", bqDataset, bqTable),
			Origin: "CreateBQTable",
			Code:   bu.ErrCreateTable,
			Err:    err,
		}
	}
	return nil
}

// InsertFromGCSIntoBQ pushes a GCS object to BQ
func InsertFromGCSIntoBQ(ctx context.Context, bqClient *bigquery.Client, 
						gcsBucket string, gcsObject string, bqDataset string, 
						bqTable string, bqTimePartitionField string) error {

	var err error

	dstDS := bqClient.Dataset(bqDataset)
	dstT := dstDS.Table(bqTable)
	
	gcsO := bigquery.NewGCSReference("gs://" + gcsBucket + "/" + gcsObject)
	gcsO.SourceFormat = bigquery.Parquet
	loader := dstT.LoaderFrom(gcsO)
	loader.CreateDisposition = bigquery.CreateNever
	if bqTimePartitionField != "" {
		loader.TimePartitioning = &bigquery.TimePartitioning{Expiration: 0, Field: bqTimePartitionField}
	}

	job, err := loader.Run(ctx)
	if err != nil {
		return bu.TError{
			Msg:    fmt.Sprintf("could not load data into %v.%v from %v/%v", bqDataset, bqTable, gcsBucket, gcsObject),
			Origin: "InsertFromGCSIntoBQ",
			Code:   bu.ErrGCS,
			Err:    err,
		}
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return bu.TError{
			Msg:    fmt.Sprintf("loading into %v.%v from %v/%v failed", bqDataset, bqTable, gcsBucket, gcsObject),
			Origin: "InsertFromGCSIntoBQ",
			Code:   bu.ErrLoadGCS,
			Err:    err,
		}
	}
	if status.Err() != nil {
		return bu.TError{
			Msg:    fmt.Sprintf("loading into %v.%v from %v/%v returned:\n%v", bqDataset, bqTable, gcsBucket, gcsObject, status),
			Origin: "InsertFromGCSIntoBQ",
			Code:   bu.ErrLoadGCS,
			Err:    err,
		}
	}

	return nil
}

// PushToBQ is an attemp at concurrent data upload
func PushToBQ() error {
	wp := workerpool.New(2)
	defer wp.StopWait()
	requests := []string{"alpha", "beta", "gamma", "delta", "epsilon"}

	for _, r := range requests {
		r := r
		wp.Submit(func() {
			fmt.Println("Handling request:", r)
			time.Sleep(2 * time.Second)
		})
	}

	return nil
}

// DropBQTablePartition deletes a specified partition from a table in BQ
func DropBQTablePartition(ctx context.Context, bqClient *bigquery.Client, bqDataset string, bqTable string, partition interface{}) error {
	partitionString, ok := partition.(string)
	if !ok {
		partitionTime, ok := partition.(time.Time)
		if ok {
			partitionString = partitionTime.Format("20060102")
		} else {
			return bu.TError{
				Msg:    fmt.Sprintf("invalid partition specification [%v]", partition),
				Origin: "RmBQbqTablePartition",
				Code:   bu.ErrGeneric,
				Err:    nil,
			}
		}
	}

	dstDS := bqClient.Dataset(bqDataset)
	dstT := dstDS.Table(bqTable + "$" + partitionString)
	err := dstT.Delete(ctx)
	if err != nil {
		return bu.TError{
			Msg:    fmt.Sprintf("failed to delete bqTable partition"),
			Origin: "RmBQbqTablePartition",
			Code:   bu.ErrDeleteTable,
			Err:    err,
		}
	}

	return nil
}

// DropBQTablePartitions removes partitions from bqDataset.bqTable for dates in bqPartitionsToDrop time.Time slice
func DropBQTablePartitions(ctx context.Context, bqClient *bigquery.Client, bqDataset string, bqTable string, bqPartitionsToDrop interface{}) error {
	partitions := reflect.ValueOf(bqPartitionsToDrop)
	
	if partitions.Kind() != reflect.Slice || partitions.Len() == 0 {
		return bu.TError{
			Msg:    "data is not a slice or has zero length",
			Origin: "DropBQTablePartitions",
			Code:   bu.ErrGeneric,
			Err:    nil,
		}
	}

	for i := 0; i < partitions.Len(); i++ {
		err := DropBQTablePartition(ctx, bqClient, bqDataset, bqTable, partitions.Index(i).Interface())
		if err != nil {
			return err
		}
	}

	return nil
}

// GetBQTablePartitions returns a list of partition date-strings in a BQ table
func GetBQTablePartitions(ctx context.Context, bqClient *bigquery.Client, bqDataset string, bqTable string) ([]time.Time, error) {
	ds := bqClient.Dataset(bqDataset)
	t := ds.Table(bqTable)
	tMeta, err := t.Metadata(ctx)
	if err != nil {
		if erg, ok := err.(*googleapi.Error); ok {
			if erg.Code == http.StatusNotFound && len(erg.Errors) > 0 && erg.Errors[0].Reason == "notFound" {
				return nil, bu.TError{
					Msg:    fmt.Sprintf("table %v.%v not found", bqDataset, bqTable),
					Origin: "GetBQTablePartitions",
					Code:   bu.ErrTableNotFound,
					Err:    err,
				}
			}
		}
		return nil, bu.TError{
			Msg:    fmt.Sprintf("failed to fetch metadata for %v.%v", bqDataset, bqTable),
			Origin: "GetBQTablePartitions",
			Code:   bu.ErrGetTableMeta,
			Err:    err,
		}
	}
	
	if tMeta.TimePartitioning == nil {
		return nil, bu.TError{
			Msg:    fmt.Sprintf("table %v.%v is not partitioned", bqDataset, bqTable),
			Origin: "GetBQTablePartitions",
			Code:   bu.ErrTableNotPartitioned,
			Err:    err,
		}
	} 

	partitionField := "_PARTITIONTIME"
	if tMeta.TimePartitioning.Field != "" {
		partitionField = tMeta.TimePartitioning.Field
	}

	qry := bqClient.Query(fmt.Sprintf(
		"SELECT FORMAT_DATE('%%Y%%m%%d', p) AS p, n " +
		"FROM (SELECT DATE(`%v`) AS p, COUNT(*) AS n " +
		"FROM `%v`.`%v` GROUP BY DATE(`%v`)) ORDER BY p",
		partitionField, bqDataset, bqTable, partitionField))
	
	it, err := qry.Read(ctx)
	if err != nil {
		return nil, bu.TError{
			Msg:    fmt.Sprintf("query to %v.%v failed", bqDataset, bqTable),
			Origin: "GetBQTablePartitions",
			Code:   bu.ErrDataQuery,
			Err:    err,
		}
	}

	partitions := make([]time.Time, 0, it.TotalRows)

	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, bu.TError{
				Msg:    fmt.Sprintf("query result from %v.%v is weird", bqDataset, bqTable),
				Origin: "GetBQTablePartitions",
				Code:   bu.ErrDataQuery,
				Err:    err,
			}
		}
		partitionTime, err := time.Parse("20060102", values[0].(string))
		if err != nil {
			return nil, bu.TError{
				Msg:    fmt.Sprintf("couldn't parse time returned from %v.%v", bqDataset, bqTable),
				Origin: "GetBQTablePartitions",
				Code:   bu.ErrGeneric,
				Err:    err,
			}
		}
		partitions = append(partitions, partitionTime)
	}

	return partitions, nil
}

// GetBQColumnStats returns a statistic or a set thereof on a column
func GetBQColumnStats(ctx context.Context, bqClient *bigquery.Client, stats []string, bqDataset string, bqTable string, bqColumn string) (map[string]interface{}, error) {
	ds := bqClient.Dataset(bqDataset)
	t := ds.Table(bqTable)
	tMeta, err := t.Metadata(ctx)
	if err != nil {
		return nil, bu.TError{
			Msg:    fmt.Sprintf("failed to fetch metadata of %v.%v", bqDataset, bqTable),
			Origin: "GetBQColumnStats",
			Code:   bu.ErrGetTableMeta,
			Err:    err,
		}
	}

	if !HasField(&tMeta.Schema, bqColumn) {
		return nil, bu.TError{
			Msg:    fmt.Sprintf("column %v not in %v.%v", bqColumn, bqDataset, bqTable),
			Origin: "GetBQColumnStats",
			Code:   bu.ErrGeneric,
			Err:    nil,
		}
	}

	fmt.Println("here")
	
	// qry := bqClient.Query(fmt.Sprintf(
	// 	"SELECT FORMAT_DATE('%%Y%%m%%d', p) AS p, n " +
	// 	"FROM (SELECT DATE(`%v`) AS p, COUNT(*) AS n " +
	// 	"FROM `%v`.`%v` GROUP BY DATE(`%v`)) ORDER BY p",
	// 	partitionField, bqDataset, bqTable, partitionField))
	
	// it, err := qry.Read(ctx)
	// if err != nil {
	// 	return nil, bu.TError{
	// 		Msg:    fmt.Sprintf("query to %v.%v failed", bqDataset, bqTable),
	// 		Origin: "GetBQTablePartitions",
	// 		Code:   bu.ErrDataQuery,
	// 		Err:    err,
	// 	}
	// }

	statsValues := make(map[string]interface{})
	// for {
	// 	var values []bigquery.Value
	// 	err := it.Next(&values)
	// 	if err == iterator.Done {
	// 		break
	// 	}
	// 	if err != nil {
	// 		return nil, bu.TError{
	// 			Msg:    fmt.Sprintf("query result from %v.%v is weird", bqDataset, bqTable),
	// 			Origin: "GetBQTablePartitions",
	// 			Code:   bu.ErrDataQuery,
	// 			Err:    err,
	// 		}
	// 	}
	// 	partitionTime, err := time.Parse("20060102", values[0].(string))
	// 	if err != nil {
	// 		return nil, bu.TError{
	// 			Msg:    fmt.Sprintf("couldn't parse time returned from %v.%v", bqDataset, bqTable),
	// 			Origin: "GetBQTablePartitions",
	// 			Code:   bu.ErrGeneric,
	// 			Err:    err,
	// 		}
	// 	}
	// 	partitions = append(partitions, partitionTime)
	// }

	return statsValues, nil
}