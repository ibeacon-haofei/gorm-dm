/**
 * @Description 达梦数据库 Clob (*dm.DmClob)数据类型处理
 * @Author $
 * @Date $ $
 **/
package customdbtype

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"gitee.com/chunanyong/dm"
)

type MyBlob dm.DmBlob

// 写入数据库之前，对数据做类型转换
func (blob *MyBlob) Value() (driver.Value, error) {
	if blob == nil {
		return nil, nil
	}
	return dm.DmBlob(*blob), nil
}

// 将数据库中取出的数据，赋值给目标类型
func (blob *MyBlob) Scan(v interface{}) error {
	switch v.(type) {
	case *dm.DmBlob:
		tmp := v.(*dm.DmClob)
		le, err := tmp.GetLength()
		if err != nil {
			return errors.New(fmt.Sprint("err：", err))
		}

		str, err := tmp.ReadString(1, int(le))
		if err != nil {
			return errors.New(fmt.Sprint("err：", err))
		}
		fmt.Println(str)
		val, err := tmp.Value()
		if err != nil {
			return errors.New(fmt.Sprint("err：", err))
		}
		fmt.Println(val)
		// *blob = MyBlob(val)
		break

	//非clob，当成字符串，兼容oracle
	default:
		// *blob = MyClob(v.(dm.DmBlob))
	}
	return nil
}
