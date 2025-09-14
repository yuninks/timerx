package priority

import (
	"errors"
	"math"
	"strconv"
	"strings"
)

var (
	ErrVersionFormat = errors.New("version format error")
)

// 版本号转策略等级
func PriorityByVersion(version string) (priority int64, err error) {
	// 版本不能为空
	if version == "" {
		return 0, ErrVersionFormat
	}

	// 除掉版本号中的v或V
	if version[0] == 'v' || version[0] == 'V' {
		version = version[1:]
	}
	// 用点号切割
	vs := strings.Split(version, ".")
	// 最多只支持5位
	if len(vs) > 5 {
		return 0, ErrVersionFormat
	}

	// base 16位
	var baseNum float64 = 0

	// 每一位转成数字&每一位不能大于999
	for key, val := range vs {
		if val == "" {
			return 0, ErrVersionFormat
		}
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, ErrVersionFormat
		}
		if i < 0 || i > 999 {
			return 0, ErrVersionFormat
		}
		p := (4 - key) * 3
		num := math.Pow10(p) * float64(i)

		baseNum += num

	}

	return int64(baseNum), nil
}
