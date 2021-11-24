package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	cf_names	map[string]string
	engine		engine_util.Engines // for storage
	iterators	map[string]engine_util.BadgerIterator // iterator for scan cf
	txn			badger.Txn // transition controller
	isAvailable	bool
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		cf_names: map[string]string{},
		engine:    engine_util.Engines{
			Kv:       engine_util.CreateDB(conf.DBPath, conf.Raft),
			KvPath:   conf.DBPath,
			Raft:     nil,
			RaftPath: "",
		},
		iterators: map[string]engine_util.BadgerIterator{},
		txn:       nil,
		isAvailable: false,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	if s.isAvailable {
		log.Warn("Server is now started, you can not start it again.")
		return nil
	}
	s.isAvailable=true
	s.txn=*s.engine.Kv.NewTransaction(true)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if !s.isAvailable {
		log.Warn("Server is now closed, you can not stop it.")
		return nil
	}
	s.isAvailable=false
	s.txn.Discard()
	err := s.engine.Close()
	err = s.engine.Destroy()
	if err != nil {
		log.Error("Stop server failed because "+err.Error())
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneReader{inner: s}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := new(engine_util.WriteBatch)
	for i:=0;i<len(batch);i++ {
		modify := batch[i]
		switch modify.Data.(type){
		case storage.Put:
			wb.SetCF(modify.Cf(),modify.Key(),modify.Value())
			break;
		case storage.Delete:
			wb.DeleteCF(modify.Cf(),modify.Key())
			break;
		}
	}
	return nil
}

type StandAloneReader struct {
	inner		StandAloneStorage
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCF(r.inner.engine.Kv, cf, key)
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.inner.txn)
}

func (r *StandAloneReader) Close() {
	r.txn.Discard()
}

