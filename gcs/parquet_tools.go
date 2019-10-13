package gcstools

import (
	"fmt"
	"io"
	"os"
	"time"
	"reflect"
	
	"github.com/gammazero/workerpool"

	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetWriter"
	"github.com/xitongsys/parquet-go/parquet"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
	
	bu "github.com/belboo/boo-go-tools/misc"
)

// WriteParquetGCSPar parallelizes parquet writing to GCS
func WriteParquetGCSPar() error {
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

// WriteParquetToGCS writes a slice of data to a GCS object 
func WriteParquetToGCS(ctx context.Context, data interface{}, project string, bucket string, object string, bydate bool) error {

	typedData := reflect.ValueOf(data)

	if typedData.Kind() != reflect.Slice || typedData.Len() == 0 {
		return nil
	}

	fw, err := ParquetFile.NewGcsFileWriter(ctx, project, bucket, object)
	if err != nil {
		return bu.TError{
			Msg:    fmt.Sprintf("could not instantiate a GCS File Writer for %v/%v", bucket, object),
			Origin: "WriteParquetToGCS",
			Code:   bu.ErrGCS,
			Err:    err,
		}
	}

	pw, err := ParquetWriter.NewParquetWriter(fw, reflect.New(typedData.Index(0).Type()).Interface(), 4)
	if err != nil {
		return bu.TError{
			Msg:    fmt.Sprintf("could not instantiate a Parquet Writer for %v/%v", bucket, object),
			Origin: "WriteParquetToGCS",
			Code:   bu.ErrGCS,
			Err:    err,
		}
	}

	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for i := 0; i < typedData.Len(); i++ {
		if err = pw.Write(typedData.Index(i).Interface()); err != nil {
			return bu.TError{
				Msg:    fmt.Sprintf("failed while writing data[%v] to %v/%v", i, bucket, object),
				Origin: "WriteParquetToGCS",
				Code:   bu.ErrGCS,
				Err:    err,
			}
		}
	}

	if err = pw.WriteStop(); err != nil {
		return bu.TError{
			Msg:    fmt.Sprintf("could not commit written data to %v/%v, data might be corrupted", bucket, object),
			Origin: "WriteParquetToGCS",
			Code:   bu.ErrGCS,
			Err:    err,
		}
	}

	fw.Close()

	return nil
}

// WriteParquetWithSchemaToGCS writes a slice of data to a GCS object 
func WriteParquetWithSchemaToGCS(ctx context.Context, data interface{}, project string, bucket string, object string, schema string) error {

	typedData := reflect.ValueOf(data)

	if typedData.Kind() != reflect.Slice || typedData.Len() == 0 {
		return nil
	}

	fw, err := ParquetFile.NewGcsFileWriter(ctx, project, bucket, object)
	if err != nil {
		return bu.TError{
			Msg:    fmt.Sprintf("could not instantiate a GCS File Writer for %v/%v", bucket, object),
			Origin: "WriteParquetWithSchemaToGCS",
			Code:   bu.ErrGCS,
			Err:    err,
		}
	}

	pw, err := ParquetWriter.NewParquetWriter(fw, reflect.New(typedData.Index(0).Type()).Interface(), 4)
	if err != nil {
		return bu.TError{
			Msg:    fmt.Sprintf("could not instantiate a Parquet Writer for %v/%v", bucket, object),
			Origin: "WriteParquetWithSchemaToGCS",
			Code:   bu.ErrGCS,
			Err:    err,
		}
	}
	
	if err = pw.SetSchemaHandlerFromJSON(schema); err != nil {
		return bu.TError{
			Msg:    fmt.Sprintf("could not set schema from JSON for the Parquet Writer for %v/%v", bucket, object),
			Origin: "WriteParquetWithSchemaToGCS",
			Code:   bu.ErrGCS,
			Err:    err,
		}
	}

	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for i := 0; i < typedData.Len(); i++ {
		if err = pw.Write(typedData.Index(i).Interface()); err != nil {
			return bu.TError{
				Msg:    fmt.Sprintf("failed while writing data[%v] to %v/%v", i, bucket, object),
				Origin: "WriteParquetWithSchemaToGCS",
				Code:   bu.ErrGCS,
				Err:    err,
			}
		}
	}

	if err = pw.WriteStop(); err != nil {
		return bu.TError{
			Msg:    fmt.Sprintf("could not commit written data to %v/%v, data might be corrupted", bucket, object),
			Origin: "WriteParquetWithSchemaToGCS",
			Code:   bu.ErrGCS,
			Err:    err,
		}
	}

	fw.Close()

	return nil
}

// WriteParquetWithSchemaToLocal writes a slice of data to a GCS object 
func WriteParquetWithSchemaToLocal(ctx context.Context, data interface{}, project string, bucket string, object string, schema string) error {

	typedData := reflect.ValueOf(data)

	if typedData.Kind() != reflect.Slice || typedData.Len() == 0 {
		return nil
	}

	fw, err := ParquetFile.NewLocalFileWriter("test.parquet")
	if err != nil {
		return bu.TError{
			Msg:    fmt.Sprintf("could not instantiate a GCS File Writer for %v/%v", bucket, object),
			Origin: "WriteParquetWithSchemaToGCS",
			Code:   bu.ErrGCS,
			Err:    err,
		}
	}

	pw, err := ParquetWriter.NewParquetWriter(fw, reflect.New(typedData.Index(0).Type()).Interface(), 4)
	if err != nil {
		return bu.TError{
			Msg:    fmt.Sprintf("could not instantiate a Parquet Writer for %v/%v", bucket, object),
			Origin: "WriteParquetWithSchemaToGCS",
			Code:   bu.ErrGCS,
			Err:    err,
		}
	}
	
	if err = pw.SetSchemaHandlerFromJSON(schema); err != nil {
		return bu.TError{
			Msg:    fmt.Sprintf("could not set schema from JSON for the Parquet Writer for %v/%v", bucket, object),
			Origin: "WriteParquetWithSchemaToGCS",
			Code:   bu.ErrGCS,
			Err:    err,
		}
	}

	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for i := 0; i < typedData.Len(); i++ {
		if err = pw.Write(typedData.Index(i).Interface()); err != nil {
			return bu.TError{
				Msg:    fmt.Sprintf("failed while writing data[%v] to %v/%v", i, bucket, object),
				Origin: "WriteParquetWithSchemaToGCS",
				Code:   bu.ErrGCS,
				Err:    err,
			}
		}
	}

	if err = pw.WriteStop(); err != nil {
		return bu.TError{
			Msg:    fmt.Sprintf("could not commit written data to %v/%v, data might be corrupted", bucket, object),
			Origin: "WriteParquetWithSchemaToGCS",
			Code:   bu.ErrGCS,
			Err:    err,
		}
	}

	fw.Close()

	return nil
}

// UploadToGCS is a thin envelope for GCS upload
func UploadToGCS(ctx context.Context, file string, bucket string, object string) error {
	
	f, err := os.Open(file)
	if err != nil {
		return bu.TError{
			Msg:    fmt.Sprintf("could not open %v for reading", file),
			Origin: "UploadToGCS",
			Code:   bu.ErrGCS,
			Err:    err,
		}
	}
	defer f.Close()

	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		return bu.TError{
			Msg:    fmt.Sprintf("could not instantiate a GCS client while uploading %v to %v/%v", file, bucket, object),
			Origin: "UploadToGCS",
			Code:   bu.ErrGCS,
			Err:    err,
		}
	}
	defer gcsClient.Close()

	wc := gcsClient.Bucket(bucket).Object(object).NewWriter(ctx)
	
	if _, err = io.Copy(wc, f); err != nil {
		return bu.TError{
			Msg:    fmt.Sprintf("upload failed: %v -> %v/%v", file, bucket, object),
			Origin: "UploadToGCS",
			Code:   bu.ErrGCS,
			Err:    err,
		}
	}
	if err := wc.Close(); err != nil {
		return bu.TError{
			Msg:    fmt.Sprintf("could not close %v/%v, data might be corrupted", bucket, object),
			Origin: "UploadToGCS",
			Code:   bu.ErrGCS,
			Err:    err,
		}
	}
	return nil
}