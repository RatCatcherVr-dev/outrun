package dbaccess

import (
	"errors"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/RunnersRevival/outrun/consts"
)

var db *bolt.DB
var DatabaseIsBusy = false

func Set(bucket, key string, value []byte) error {
	CheckIfDBSet()
	value = Compress(value) // compress the input first
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		return b.Put([]byte(key), value)
	})
	return err
}

func Get(bucket, key string) ([]byte, error) {
	CheckIfDBSet()
	var value []byte
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return errors.New("bucket '" + bucket + "' does not exist")
		}
		
		v := b.Get([]byte(key))
		if v == nil {
			return errors.New("no value named '" + key + "' in bucket '" + bucket + "'")
		}
		
		// Copy the byte slice so it remains valid outside the transaction
		value = make([]byte, len(v))
		copy(value, v)
		return nil
	})

	if err != nil {
		return nil, err
	}

	result, derr := Decompress(value) // decompress the result
	if derr != nil {
		return result, derr
	}
	return result, nil
}

func Delete(bucket, key string) error {
	CheckIfDBSet()
	// Must use db.Update since deleting mutates the database
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return nil // Bucket doesn't exist, nothing to delete
		}
		return b.Delete([]byte(key))
	})
}

func ForEachKey(bucket string, each func(k, v []byte) error) error {
	CheckIfDBSet()
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return nil // Bucket doesn't exist yet
		}
		return b.ForEach(each)
	})
	return err
}

func ForEachLogic(each func(tx *bolt.Tx) error) error {
	CheckIfDBSet()
	err := db.View(each)
	return err
}

func CheckIfDBSet() {
	if db == nil {
		bdb, err := bolt.Open(consts.DBFileName, 0600, &bolt.Options{Timeout: 3 * time.Second})
		if err != nil {
			panic(err)
		}
		db = bdb
	}
}
