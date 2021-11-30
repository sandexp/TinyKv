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
	key := req.Key
	cf := req.Cf
	reader, err := server.storage.Reader(nil)
	val, err := reader.GetCF(cf, key)

	if err != nil {
		return &kvrpcpb.RawGetResponse{
			RegionError:          nil,
			Error:                err.Error(),
			Value:                nil,
			NotFound:             true,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		}, nil
	}
	if val==nil{
		return &kvrpcpb.RawGetResponse{
			RegionError:          nil,
			Error:                "",
			Value:                nil,
			NotFound:             true,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		}, nil
	}

	return &kvrpcpb.RawGetResponse{
		RegionError:          nil,
		Error:                "",
		Value:                val,
		NotFound:             false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	cf := req.Cf
	key := req.Key
	value := req.Value

	put := storage.Put{
		Key:   key,
		Value: value,
		Cf:    cf,
	}
	var modify storage.Modify
	modify.Data=put
	err := server.storage.Write(nil, []storage.Modify{modify})
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	cf := req.Cf
	key := req.Key
	delete := storage.Delete{
		Key: key,
		Cf:  cf,
	}
	var modify storage.Modify
	modify.Data=delete
	err := server.storage.Write(nil, []storage.Modify{modify})
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	cf := req.Cf
	startKey := req.StartKey
	limit := req.Limit
	reader, err := server.storage.Reader(nil)
	iterator := reader.IterCF(cf)

	if err != nil {
		return nil, err
	}
	iterator.Seek(startKey)
	kvs:= make([]*kvrpcpb.KvPair,0)
	for i:=uint32(0);i<limit;i++ {
		if !iterator.Valid() {
			break
		}
		var key	[]byte=iterator.Item().Key()
		value, err := iterator.Item().Value()
		if err != nil {
			return nil, err
		}
		iterator.Next()
		if value!=nil {
			var kv *kvrpcpb.KvPair=new(kvrpcpb.KvPair)
			kv.Key=key
			kv.Value=value
			kvs=append(kvs,kv)
		}

	}
	return &kvrpcpb.RawScanResponse{
		RegionError:          nil,
		Error:                "",
		Kvs:                  kvs,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}, nil
}
