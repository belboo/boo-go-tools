package gcstools

import (
	"fmt"
	
	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
	
	bu "github.com/belboo/boo-go-tools/misc"
)

// RmObject is a thin envelope for GCS remove
func RmObject(ctx context.Context, bucket string, object string) error {
	
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		return bu.TError{
			Msg:    fmt.Sprintf("could not instantiate a GCS client"),
			Origin: "gcs.RmObject",
			Code:   bu.ErrGCS,
			Err:    err,
		}
	}
	defer gcsClient.Close()

	if err := gcsClient.Bucket(bucket).Object(object).Delete(ctx); err != nil {
        return bu.TError{
			Msg:    fmt.Sprintf("failed to delete %v/%v", bucket, object),
			Origin: "gcs.RmObject",
			Code:   bu.ErrGCS,
			Err:    err,
		}
	}

	return nil
}