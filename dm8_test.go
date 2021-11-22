package main

import (
	"fmt"
	"github.com/ibeacon-haofei/gorm-dm/dameng"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	"log"
	"os"
	"testing"
	"time"
)

type AlarmInfo struct {
	ID          int       `json:"id" gorm:"column:"id"`
	Name        string    `json:"name" gorm:"column:"name"`
	Description string    `json:"description" gorm:"column:"description"`
	UpdateTime  time.Time `json:"updateTime" gorm:"column:"update_time"`
	CreateTime  time.Time `json:"createTime" gorm:"column:"create_time"`
	Version     int       `json:"version" gorm:"column:"version"`
	Status      int       `json:"status" gorm:"column:"status"`
	MessageNbr  string    `json:"messageNbr" gorm:"column:"message_nbr"`
	AlarmRuleID int       `json:"alarmRuleID" gorm:"column:"alarm_rule_id"`
}

var (
	db  *gorm.DB
	err error
)

func init() {
	dsn := "dm://SYSDBA:SYSDBA@192.168.10.11:5236?autoCommit=true"

	db, err = gorm.Open(dameng.Open(dsn), &gorm.Config{
		Logger: logger.New(log.New(os.Stdout, "\r\n", log.LstdFlags), logger.Config{
			SlowThreshold: 1 * time.Millisecond,
			LogLevel:      logger.Warn,
			Colorful:      true,
		}),
		DisableForeignKeyConstraintWhenMigrating: true,
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   "SYSDBA.",
			SingularTable: true, //表名后面不加s
		},
	})
	if err != nil {
		fmt.Println(err)
	}
}
func add() error {
	a := &AlarmInfo{
		Name:        "我是新增的数据",
		Description: "测试",
		UpdateTime:  time.Now(),
		CreateTime:  time.Now(),
		Version:     0,
		Status:      0,
		MessageNbr:  "hello",
		AlarmRuleID: 18,
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Debug().Table("water.alarm_info").Create(&a).Error; err != nil {
			fmt.Println(err)
		}
		// 返回 nil 提交事务
		return nil
	})
	if err != nil {
		return err
	}
	return err
}
func (AlarmInfo) TableName() string {
	return "water.alarm_info"
}

func TestDM8(t *testing.T) {
	t.Log(add())
}
