package storage

// Placeholder for future persistence implementation

type RocksDBStore struct{}

func NewRocksDBStore() *RocksDBStore {
	return &RocksDBStore{}
}

func (r *RocksDBStore) Put(key, value string) {}
func (r *RocksDBStore) Get(key string) (string, bool) { return "", false }
func (r *RocksDBStore) Delete(key string) {}