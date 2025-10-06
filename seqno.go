package main

import (
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func storeLastSeq(db *gorm.DB, key string, seq int64) error {
	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}},
		DoUpdates: clause.AssignmentColumns([]string{"int_val"}),
	}).Create(&SequenceTracker{
		Key:    key,
		IntVal: seq,
	}).Error
}

func loadLastSeq(db *gorm.DB, key string) (int64, error) {
	var info SequenceTracker
	if err := db.Where("key = ?", key).First(&info).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return 0, nil
		}
		return 0, err
	}
	return info.IntVal, nil
}
