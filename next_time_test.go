package timerx_test

import (
	"errors"
	"testing"
	"time"

	"github.com/yuninks/timerx"
)

func TestGetNextTime(t *testing.T) {
	// Test cases
	tests := []struct {
		name          string
		job           timerx.JobData
		expectedTime  time.Time
		expectedError error
	}{
		{
			name: "Test JobTypeEveryMonth",
			job: timerx.JobData{
				JobType: timerx.JobTypeEveryMonth,
				Day:     15,
				Hour:    10,
				Minute:  0,
				Second:  0,
			},
			expectedTime:  time.Date(2022, 3, 15, 10, 0, 0, 0, time.Local),
			expectedError: nil,
		},
		{
			name: "Test JobTypeEveryWeek",
			job: timerx.JobData{
				JobType: timerx.JobTypeEveryWeek,
				Weekday: time.Tuesday,
				Hour:    10,
				Minute:  0,
				Second:  0,
			},
			expectedTime:  time.Date(2022, 3, 8, 10, 0, 0, 0, time.Local), // Assuming current date is March 7, 2022
			expectedError: nil,
		},
		{
			name: "Test JobTypeEveryDay",
			job: timerx.JobData{
				JobType: timerx.JobTypeEveryDay,
				Hour:    10,
				Minute:  0,
				Second:  0,
			},
			expectedTime:  time.Date(2022, 3, 8, 10, 0, 0, 0, time.Local), // Assuming current date is March 7, 2022
			expectedError: nil,
		},
		{
			name: "Test JobTypeEveryHour",
			job: timerx.JobData{
				JobType: timerx.JobTypeEveryHour,
				Minute:  0,
				Second:  0,
			},
			expectedTime:  time.Date(2022, 3, 7, 11, 0, 0, 0, time.Local), // Assuming current date is March 7, 2022, 10:30 AM
			expectedError: nil,
		},
		{
			name: "Test JobTypeEveryMinute",
			job: timerx.JobData{
				JobType: timerx.JobTypeEveryMinute,
				Second:  0,
			},
			expectedTime:  time.Date(2022, 3, 7, 10, 31, 0, 0, time.Local), // Assuming current date is March 7, 2022, 10:30 AM
			expectedError: nil,
		},
		{
			name: "Test JobTypeInterval",
			job: timerx.JobData{
				JobType:      timerx.JobTypeInterval,
				IntervalTime: 1 * time.Hour,
			},
			expectedTime:  time.Date(2022, 3, 7, 11, 30, 0, 0, time.Local), // Assuming current date is March 7, 2022, 10:30 AM
			expectedError: nil,
		},
		{
			name: "Test unknown JobType",
			job: timerx.JobData{
				JobType: timerx.JobType(100),
			},
			expectedTime:  time.Time{},
			expectedError: errors.New("未知的任务类型: 100"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now()
			loc := time.FixedZone("CST", 8*3600)
			nextTime, err := timerx.GetNextTime(now,loc,test.job)
			if err != nil {
				if test.expectedError == nil || err.Error() != test.expectedError.Error() {
					t.Errorf("Expected error: %v, Got error: %v", test.expectedError, err)
				}
			} else {
				if nextTime.IsZero() != (test.expectedTime == time.Time{}) || (nextTime != &test.expectedTime) {
					t.Errorf("Expected time: %v, Got time: %v", test.expectedTime, nextTime)
				}
			}
		})
	}
}
