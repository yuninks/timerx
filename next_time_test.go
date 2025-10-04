package timerx

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetNextTime(t *testing.T) {

	tt := time.Date(2025, 10, 16, 10, 30, 5, 0, time.Local)

	// Test cases
	tests := []struct {
		name          string
		job           JobData
		expectedTime  time.Time
		expectedError error
	}{
		{
			name: "Test JobTypeEveryMonth",
			job: JobData{
				JobType: JobTypeEveryMonth,
				Day:     15,
				Hour:    10,
				Minute:  0,
				Second:  0,
			},
			expectedTime:  time.Date(tt.Year(), tt.Month()+1, 15, 10, 0, 0, 0, time.Local),
			expectedError: nil,
		},
		{
			name: "Test JobTypeEveryWeek",
			job: JobData{
				JobType: JobTypeEveryWeek,
				Weekday: time.Tuesday,
				Hour:    10,
				Minute:  0,
				Second:  0,
			},
			expectedTime:  time.Date(2025, 10, 21, 10, 0, 0, 0, time.Local), // Assuming current date is March 7, 2022
			expectedError: nil,
		},
		{
			name: "Test JobTypeEveryDay",
			job: JobData{
				JobType: JobTypeEveryDay,
				Hour:    10,
				Minute:  0,
				Second:  0,
			},
			expectedTime:  time.Date(2025, 10, 17, 10, 0, 0, 0, time.Local), // Assuming current date is March 7, 2022
			expectedError: nil,
		},
		{
			name: "Test JobTypeEveryHour",
			job: JobData{
				JobType: JobTypeEveryHour,
				Minute:  0,
				Second:  0,
			},
			expectedTime:  time.Date(2025, 10, 16, 11, 0, 0, 0, time.Local), // Assuming current date is March 7, 2022, 10:30 AM
			expectedError: nil,
		},
		{
			name: "Test JobTypeEveryMinute",
			job: JobData{
				JobType: JobTypeEveryMinute,
				Second:  12,
			},
			expectedTime:  time.Date(tt.Year(), tt.Month(), tt.Day(), tt.Hour(), tt.Minute(), 12, 0, time.Local), // Assuming current date is March 7, 2022, 10:30 AM
			expectedError: nil,
		},
		{
			name: "Test JobTypeIntervalHour",
			job: JobData{
				JobType:      JobTypeInterval,
				BaseTime:     tt,
				IntervalTime: 1 * time.Hour,
			},
			expectedTime: time.Date(2025, 10, 16, 12, 00, 0, 0, time.Local), // Assuming current date is March 7, 2022, 10:30 AM
			expectedError: nil,
		},
		{
			name: "Test JobTypeIntervalMinute",
			job: JobData{
				JobType:      JobTypeInterval,
				BaseTime:     tt,
				IntervalTime: 1 * time.Minute,
			},
			expectedTime:  time.Date(2025, 10, 16, 10, 31, 0, 0, time.Local), // Assuming current date is March 7, 2022, 10:30 AM
			expectedError: nil,
		},
		{
			name: "Test JobTypeIntervalSecond",
			job: JobData{
				JobType:      JobTypeInterval,
				BaseTime:     tt,
				IntervalTime: 1 * time.Second,
			},
			expectedTime:  tt.Add(1 * time.Second), // Assuming current date is March 7, 2022, 10:30 AM
			expectedError: nil,
		},
		{
			name: "Test unknown JobType",
			job: JobData{
				JobType: JobType("100"),
			},
			expectedTime:  time.Time{},
			expectedError: errors.New("未知的任务类型: 100"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// loc := time.FixedZone("CST", 8*3600)
			nextTime, err := GetNextTime(tt, test.job)
			if err != nil {
				if test.expectedError == nil || err.Error() != test.expectedError.Error() {
					t.Errorf("Expected error: %v, Got error: %v", test.expectedError, err)
				}
			} else {
				if nextTime.IsZero() != (test.expectedTime == time.Time{}) || (!nextTime.Equal(test.expectedTime)) {
					t.Errorf("Expected time: %v, Got time: %v", test.expectedTime, nextTime)
				}
			}
		})
	}
}

// 测试参数验证
func TestValidateJobData(t *testing.T) {
	tests := []struct {
		name     string
		job      JobData
		expected error
	}{
		{
			name: "有效月任务",
			job: JobData{
				JobType: JobTypeEveryMonth,
				Day:     15,
				Hour:    12,
				Minute:  30,
				Second:  0,
			},
			expected: nil,
		},
		{
			name: "无效月任务-日期太小",
			job: JobData{
				JobType: JobTypeEveryMonth,
				Day:     0,
				Hour:    12,
				Minute:  30,
				Second:  0,
			},
			expected: ErrMonthDay,
		},
		{
			name: "无效月任务-日期太大",
			job: JobData{
				JobType: JobTypeEveryMonth,
				Day:     32,
				Hour:    12,
				Minute:  30,
				Second:  0,
			},
			expected: ErrMonthDay,
		},
		{
			name: "有效周任务",
			job: JobData{
				JobType: JobTypeEveryWeek,
				Weekday: time.Monday,
				Hour:    12,
				Minute:  30,
				Second:  0,
			},
			expected: nil,
		},
		{
			name: "无效周任务-星期超出范围",
			job: JobData{
				JobType: JobTypeEveryWeek,
				Weekday: time.Weekday(7), // 超出范围
				Hour:    12,
				Minute:  30,
				Second:  0,
			},
			expected: ErrWeekday,
		},
		{
			name: "有效间隔任务",
			job: JobData{
				JobType:      JobTypeInterval,
				BaseTime:     time.Now(),
				IntervalTime: time.Minute,
				Hour:         12,
				Minute:       30,
				Second:       0,
			},
			expected: nil,
		},
		{
			name: "无效间隔任务-间隔时间为0",
			job: JobData{
				JobType:      JobTypeInterval,
				BaseTime:     time.Now(),
				IntervalTime: 0,
				Hour:         12,
				Minute:       30,
				Second:       0,
			},
			expected: ErrIntervalTime,
		},
		{
			name: "无效间隔任务-创建时间为空",
			job: JobData{
				JobType:      JobTypeInterval,
				BaseTime:     time.Time{},
				IntervalTime: time.Minute,
				Hour:         12,
				Minute:       30,
				Second:       0,
			},
			expected: ErrBaseTime,
		},
		{
			name: "无效小时",
			job: JobData{
				JobType: JobTypeEveryDay,
				Hour:    24, // 无效小时
				Minute:  30,
				Second:  0,
			},
			expected: ErrHour,
		},
		{
			name: "无效分钟",
			job: JobData{
				JobType: JobTypeEveryDay,
				Hour:    12,
				Minute:  60, // 无效分钟
				Second:  0,
			},
			expected: ErrMinute,
		},
		{
			name: "无效秒数",
			job: JobData{
				JobType: JobTypeEveryDay,
				Hour:    12,
				Minute:  30,
				Second:  60, // 无效秒数
			},
			expected: ErrSecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateJobData(tt.job)
			assert.Equal(t, tt.expected, err)
		})
	}
}

// 测试间隔任务
func TestCalculateNextInterval(t *testing.T) {
	now := time.Date(2023, 6, 15, 12, 0, 0, 0, time.UTC)
	createTime := time.Date(2023, 6, 15, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		job         JobData
		currentTime time.Time
		expected    time.Time
	}{
		{
			name: "间隔1小时-当前时间在创建时间之后",
			job: JobData{
				JobType:      JobTypeInterval,
				BaseTime:     createTime,
				IntervalTime: time.Hour,
			},
			currentTime: now,
			expected:    time.Date(2023, 6, 15, 13, 0, 0, 0, time.UTC),
		},
		{
			name: "间隔30分钟-刚好在间隔点上",
			job: JobData{
				JobType:      JobTypeInterval,
				BaseTime:     createTime,
				IntervalTime: 30 * time.Minute,
			},
			currentTime: time.Date(2023, 6, 15, 12, 30, 0, 0, time.UTC),
			expected:    time.Date(2023, 6, 15, 13, 0, 0, 0, time.UTC),
		},
		{
			name: "间隔1天-跨天",
			job: JobData{
				JobType:      JobTypeInterval,
				BaseTime:     createTime,
				IntervalTime: 24 * time.Hour,
			},
			currentTime: now,
			expected:    time.Date(2023, 6, 16, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := calculateNextInterval(tt.currentTime, tt.job)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, *result)
		})
	}
}

// 测试月任务
func TestCalculateNextMonthTime(t *testing.T) {
	baseTime := time.Date(2023, 6, 15, 12, 30, 45, 0, time.UTC)

	tests := []struct {
		name        string
		job         JobData
		currentTime time.Time
		expected    time.Time
	}{
		{
			name: "本月还能执行",
			job: JobData{
				JobType: JobTypeEveryMonth,
				Day:     20,
				Hour:    12,
				Minute:  30,
				Second:  45,
			},
			currentTime: baseTime,
			expected:    time.Date(2023, 6, 20, 12, 30, 45, 0, time.UTC),
		},
		{
			name: "本月已过，下个月执行",
			job: JobData{
				JobType: JobTypeEveryMonth,
				Day:     10,
				Hour:    12,
				Minute:  30,
				Second:  45,
			},
			currentTime: baseTime,
			expected:    time.Date(2023, 7, 10, 12, 30, 45, 0, time.UTC),
		},
		{
			name: "2月30日调整到2月28日",
			job: JobData{
				JobType: JobTypeEveryMonth,
				Day:     30,
				Hour:    12,
				Minute:  30,
				Second:  45,
			},
			currentTime: time.Date(2023, 2, 15, 12, 30, 45, 0, time.UTC),
			expected:    time.Date(2023, 2, 28, 12, 30, 45, 0, time.UTC),
		},
		{
			name: "闰年2月29日",
			job: JobData{
				JobType: JobTypeEveryMonth,
				Day:     29,
				Hour:    12,
				Minute:  30,
				Second:  45,
			},
			currentTime: time.Date(2024, 2, 15, 12, 30, 45, 0, time.UTC), // 2024是闰年
			expected:    time.Date(2024, 2, 29, 12, 30, 45, 0, time.UTC),
		},
		{
			name: "跨年",
			job: JobData{
				JobType: JobTypeEveryMonth,
				Day:     15,
				Hour:    12,
				Minute:  30,
				Second:  45,
			},
			currentTime: time.Date(2023, 12, 20, 12, 30, 45, 0, time.UTC),
			expected:    time.Date(2024, 1, 15, 12, 30, 45, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := calculateNextMonthTime(tt.currentTime, tt.job)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, *result)
		})
	}
}

func TestCalculateNextMonthTimeOnce(t *testing.T) {
	// baseTime := time.Date(2023, 6, 15, 12, 30, 45, 0, time.UTC)

	tests := []struct {
		name        string
		job         JobData
		currentTime time.Time
		expected    time.Time
	}{
		{
			name: "2月30日调整到2月28日",
			job: JobData{
				JobType: JobTypeEveryMonth,
				Day:     30,
				Hour:    12,
				Minute:  30,
				Second:  45,
			},
			currentTime: time.Date(2023, 1, 31, 12, 30, 45, 0, time.UTC),
			expected:    time.Date(2023, 2, 28, 12, 30, 45, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := calculateNextMonthTime(tt.currentTime, tt.job)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, *result)
		})
	}
}

// 测试周任务
func TestCalculateNextWeekTime(t *testing.T) {
	baseTime := time.Date(2023, 6, 15, 12, 30, 45, 0, time.UTC) // 星期四

	tests := []struct {
		name        string
		job         JobData
		currentTime time.Time
		expected    time.Time
	}{
		{
			name: "本周还能执行-周五",
			job: JobData{
				JobType: JobTypeEveryWeek,
				Weekday: time.Friday,
				Hour:    12,
				Minute:  30,
				Second:  45,
			},
			currentTime: baseTime,
			expected:    time.Date(2023, 6, 16, 12, 30, 45, 0, time.UTC),
		},
		{
			name: "本周已过，下周执行-周三",
			job: JobData{
				JobType: JobTypeEveryWeek,
				Weekday: time.Wednesday,
				Hour:    12,
				Minute:  30,
				Second:  45,
			},
			currentTime: baseTime,
			expected:    time.Date(2023, 6, 21, 12, 30, 45, 0, time.UTC),
		},
		{
			name: "同一天但时间已过",
			job: JobData{
				JobType: JobTypeEveryWeek,
				Weekday: time.Thursday,
				Hour:    10, // 早于当前时间
				Minute:  30,
				Second:  45,
			},
			currentTime: baseTime,
			expected:    time.Date(2023, 6, 22, 10, 30, 45, 0, time.UTC),
		},
		{
			name: "跨月",
			job: JobData{
				JobType: JobTypeEveryWeek,
				Weekday: time.Monday,
				Hour:    12,
				Minute:  30,
				Second:  45,
			},
			currentTime: time.Date(2023, 6, 30, 12, 30, 45, 0, time.UTC), // 周五
			expected:    time.Date(2023, 7, 3, 12, 30, 45, 0, time.UTC),  // 下周一
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := calculateNextWeekTime(tt.currentTime, tt.job)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, *result)
		})
	}
}

// 测试日任务
func TestCalculateNextDayTime(t *testing.T) {
	baseTime := time.Date(2023, 6, 15, 12, 30, 45, 0, time.UTC)

	tests := []struct {
		name        string
		job         JobData
		currentTime time.Time
		expected    time.Time
	}{
		{
			name: "今天还能执行",
			job: JobData{
				JobType: JobTypeEveryDay,
				Hour:    14,
				Minute:  30,
				Second:  45,
			},
			currentTime: baseTime,
			expected:    time.Date(2023, 6, 15, 14, 30, 45, 0, time.UTC),
		},
		{
			name: "今天已过，明天执行",
			job: JobData{
				JobType: JobTypeEveryDay,
				Hour:    10,
				Minute:  30,
				Second:  45,
			},
			currentTime: baseTime,
			expected:    time.Date(2023, 6, 16, 10, 30, 45, 0, time.UTC),
		},
		{
			name: "跨月",
			job: JobData{
				JobType: JobTypeEveryDay,
				Hour:    10,
				Minute:  30,
				Second:  45,
			},
			currentTime: time.Date(2023, 6, 30, 12, 30, 45, 0, time.UTC),
			expected:    time.Date(2023, 7, 1, 10, 30, 45, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := calculateNextDayTime(tt.currentTime, tt.job)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, *result)
		})
	}
}

// 测试小时任务
func TestCalculateNextHourTime(t *testing.T) {
	baseTime := time.Date(2023, 6, 15, 12, 30, 45, 0, time.UTC)

	tests := []struct {
		name        string
		job         JobData
		currentTime time.Time
		expected    time.Time
	}{
		{
			name: "本小时还能执行",
			job: JobData{
				JobType: JobTypeEveryHour,
				Minute:  45,
				Second:  0,
			},
			currentTime: baseTime,
			expected:    time.Date(2023, 6, 15, 12, 45, 0, 0, time.UTC),
		},
		{
			name: "本小时已过，下小时执行",
			job: JobData{
				JobType: JobTypeEveryHour,
				Minute:  15,
				Second:  0,
			},
			currentTime: baseTime,
			expected:    time.Date(2023, 6, 15, 13, 15, 0, 0, time.UTC),
		},
		{
			name: "跨天",
			job: JobData{
				JobType: JobTypeEveryHour,
				Minute:  15,
				Second:  0,
			},
			currentTime: time.Date(2023, 6, 15, 23, 30, 45, 0, time.UTC),
			expected:    time.Date(2023, 6, 16, 0, 15, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := calculateNextHourTime(tt.currentTime, tt.job)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, *result)
		})
	}
}

// 测试分钟任务
func TestCalculateNextMinuteTime(t *testing.T) {
	baseTime := time.Date(2023, 6, 15, 12, 30, 45, 0, time.UTC)

	tests := []struct {
		name        string
		job         JobData
		currentTime time.Time
		expected    time.Time
	}{
		{
			name: "本分钟还能执行",
			job: JobData{
				JobType: JobTypeEveryMinute,
				Second:  50,
			},
			currentTime: baseTime,
			expected:    time.Date(2023, 6, 15, 12, 30, 50, 0, time.UTC),
		},
		{
			name: "本分钟已过，下分钟执行",
			job: JobData{
				JobType: JobTypeEveryMinute,
				Second:  30,
			},
			currentTime: baseTime,
			expected:    time.Date(2023, 6, 15, 12, 31, 30, 0, time.UTC),
		},
		{
			name: "跨小时",
			job: JobData{
				JobType: JobTypeEveryMinute,
				Second:  30,
			},
			currentTime: time.Date(2023, 6, 15, 12, 59, 45, 0, time.UTC),
			expected:    time.Date(2023, 6, 15, 13, 0, 30, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := calculateNextMinuteTime(tt.currentTime, tt.job)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, *result)
		})
	}
}

// 测试GetNextTime集成函数
func TestGetNextTime_Integration(t *testing.T) {
	now := time.Date(2023, 6, 15, 12, 30, 45, 0, time.UTC)

	tests := []struct {
		name     string
		job      JobData
		expected time.Time
	}{
		{
			name: "月任务集成测试",
			job: JobData{
				JobType: JobTypeEveryMonth,
				Day:     20,
				Hour:    12,
				Minute:  30,
				Second:  45,
			},
			expected: time.Date(2023, 6, 20, 12, 30, 45, 0, time.UTC),
		},
		{
			name: "周任务集成测试",
			job: JobData{
				JobType: JobTypeEveryWeek,
				Weekday: time.Friday,
				Hour:    12,
				Minute:  30,
				Second:  45,
			},
			expected: time.Date(2023, 6, 16, 12, 30, 45, 0, time.UTC),
		},
		{
			name: "间隔任务集成测试",
			job: JobData{
				JobType:      JobTypeInterval,
				BaseTime:     time.Date(2023, 6, 15, 10, 0, 0, 0, time.UTC),
				IntervalTime: time.Hour,
			},
			expected: time.Date(2023, 6, 15, 13, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GetNextTime(now, tt.job)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, *result)
		})
	}
}

// 测试错误情况
func TestGetNextTime_ErrorCases(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name     string
		job      JobData
		expected error
	}{
		{
			name: "未知任务类型",
			job: JobData{
				JobType: "99", // 无效类型
			},
			expected: errors.New("未知的任务类型: 99"),
		},
		{
			name: "无效月任务日期",
			job: JobData{
				JobType: JobTypeEveryMonth,
				Day:     32, // 无效日期
				Hour:    12,
				Minute:  30,
				Second:  0,
			},
			expected: ErrMonthDay,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GetNextTime(now, tt.job)
			assert.Nil(t, result)
			assert.Equal(t, tt.expected, err)
		})
	}
}

// 测试边界条件
func TestGetNextTime_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		job         JobData
		currentTime time.Time
		expected    time.Time
	}{
		{
			name: "刚好在执行时间点上-应该到下一个周期",
			job: JobData{
				JobType: JobTypeEveryDay,
				Hour:    12,
				Minute:  30,
				Second:  45,
			},
			currentTime: time.Date(2023, 6, 15, 12, 30, 45, 0, time.UTC),
			expected:    time.Date(2023, 6, 16, 12, 30, 45, 0, time.UTC),
		},
		{
			name: "刚好在执行时间点上-应该到下一个周期-秒",
			job: JobData{
				JobType: JobTypeEveryDay,
				Hour:    12,
				Minute:  30,
				Second:  45,
			},
			currentTime: time.Date(2023, 6, 16, 12, 30, 44, 0, time.UTC),
			expected:    time.Date(2023, 6, 16, 12, 30, 45, 0, time.UTC),
		},
		{
			name: "闰年2月29日",
			job: JobData{
				JobType: JobTypeEveryMonth,
				Day:     29,
				Hour:    12,
				Minute:  30,
				Second:  45,
			},
			currentTime: time.Date(2024, 2, 15, 12, 30, 45, 0, time.UTC), // 闰年
			expected:    time.Date(2024, 2, 29, 12, 30, 45, 0, time.UTC),
		},
		{
			name: "非闰年2月29日调整到28日",
			job: JobData{
				JobType: JobTypeEveryMonth,
				Day:     29,
				Hour:    12,
				Minute:  30,
				Second:  45,
			},
			currentTime: time.Date(2023, 2, 15, 12, 30, 45, 0, time.UTC), // 非闰年
			expected:    time.Date(2023, 2, 28, 12, 30, 45, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GetNextTime(tt.currentTime, tt.job)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, *result)
		})
	}
}

// 测试时区处理
func TestGetNextTime_Timezone(t *testing.T) {
	// 测试不同时区
	locations := []*time.Location{
		time.UTC,
		time.FixedZone("TEST+8", 8*60*60),
		time.FixedZone("TEST-5", -5*60*60),
	}

	for _, loc := range locations {
		t.Run(loc.String(), func(t *testing.T) {
			currentTime := time.Date(2023, 6, 15, 12, 30, 45, 0, loc)

			job := JobData{
				JobType: JobTypeEveryDay,
				Hour:    14,
				Minute:  30,
				Second:  45,
			}

			result, err := GetNextTime(currentTime, job)
			assert.NoError(t, err)

			expected := time.Date(2023, 6, 15, 14, 30, 45, 0, loc)
			assert.Equal(t, expected, *result)
			assert.Equal(t, loc, result.Location())
		})
	}
}
