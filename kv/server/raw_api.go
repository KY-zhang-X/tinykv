package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	resp := new(kvrpcpb.RawGetResponse)

	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}

	if value != nil {
		resp.Value = value
	} else {
		resp.NotFound = true
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	resp := new(kvrpcpb.RawPutResponse)

	put := storage.Put{Key: req.GetKey(), Value: req.GetValue(), Cf: req.GetCf()}
	modify := storage.Modify{Data: put}
	if err := server.storage.Write(req.GetContext(), []storage.Modify{modify}); err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	resp := new(kvrpcpb.RawDeleteResponse)

	delete := storage.Delete{Key: req.GetKey(), Cf: req.GetCf()}
	modify := storage.Modify{Data: delete}
	if err := server.storage.Write(req.GetContext(), []storage.Modify{modify}); err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	resp := new(kvrpcpb.RawScanResponse)

	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}

	iter := reader.IterCF(req.GetCf())
	count := 0
	for iter.Seek(req.GetStartKey()); iter.Valid() && count < int(req.GetLimit()); iter.Next() {
		key := iter.Item().KeyCopy(nil)
		value, err := iter.Item().ValueCopy(nil)
		if err != nil {
			panic(err)
		}
		kv := &kvrpcpb.KvPair{Key: key, Value: value}
		resp.Kvs = append(resp.Kvs, kv)
		count += 1
	}

	return resp, nil
}
