package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yuninks/timerx"
)

var save_path = "./cluster.log"

func main() {

	ctx := context.Background()
	client := getRedis()

	ops := []timerx.Option{}

	clu, err := timerx.InitCluster(ctx, client, "cluster", ops...)
	if err != nil {
		panic(err)
	}

	space(ctx, clu)
	minute(ctx, clu)
	hour(ctx, clu)
	day(ctx, clu)
	week(ctx, clu)
	month(ctx, clu)
	cron(ctx, clu)

	select {}

}

// space
func space(ctx context.Context, clu *timerx.Cluster) {
	// 每秒执行一次
	err := clu.EverySpace(ctx, "space_test_second", 1*time.Second, callback, "space 这是秒任务")
	fmt.Println(err)
	// 每分钟执行一次
	err = clu.EverySpace(ctx, "space_test_minute", 1*time.Minute, callback, "space 这是分钟任务")
	fmt.Println(err)
	// 每小时执行一次
	err = clu.EverySpace(ctx, "space_test_hour", 1*time.Hour, callback, "space 这是小时任务")
	fmt.Println(err)
	// 每天执行一次
	err = clu.EverySpace(ctx, "space_test_day", 24*time.Hour, callback, "space 这是天任务")
	fmt.Println(err)
	// 每周执行一次
	err = clu.EverySpace(ctx, "space_test_week", 7*24*time.Hour, callback, "space 这是周任务")
	fmt.Println(err)
	// 每月执行一次
	err = clu.EverySpace(ctx, "space_test_month", 30*24*time.Hour, callback, "space 这是月任务")
	fmt.Println(err)

}

// minute
func minute(ctx context.Context, clu *timerx.Cluster) {
	// 每分钟0s执行一次
	err := clu.EveryMinute(ctx, "minute_test_min1", 0, callback, "minute 这是分钟任务0")
	fmt.Println(err)
	// 每分钟5s执行一次
	err = clu.EveryMinute(ctx, "minute_test_min5", 5, callback, "minute 这是分钟任务5")
	fmt.Println(err)
	// 每分钟10s执行一次
	err = clu.EveryMinute(ctx, "minute_test_min10", 10, callback, "minute 这是分钟任务10")
	fmt.Println(err)
	// 每分钟15s执行一次
	err = clu.EveryMinute(ctx, "minute_test_min15", 15, callback, "minute 这是分钟任务15")
	fmt.Println(err)
	// 每分钟30s执行一次
	err = clu.EveryMinute(ctx, "minute_test_min30", 30, callback, "minute 这是分钟任务30")
	fmt.Println(err)
	// 每分钟45s执行一次
	err = clu.EveryMinute(ctx, "minute_test_min45", 45, callback, "minute 这是分钟任务45")
	fmt.Println(err)
	// 每分钟50s执行一次
	err = clu.EveryMinute(ctx, "minute_test_min50", 50, callback, "minute 这是分钟任务50")
	fmt.Println(err)
	// 每分钟55s执行一次
	err = clu.EveryMinute(ctx, "minute_test_min55", 55, callback, "minute 这是分钟任务55")
	fmt.Println(err)
}

// Hour
func hour(ctx context.Context, clu *timerx.Cluster) {
	// 每小时的第0分钟15s执行一次
	err := clu.EveryHour(ctx, "hour_test_hour1", 0, 15, callback, "hour 这是小时任务1")
	fmt.Println(err)
	// 每小时的第5分钟30s执行一次
	err = clu.EveryHour(ctx, "hour_test_hour2", 5, 30, callback, "hour 这是小时任务2")
	fmt.Println(err)
	// 每小时的第10分钟45s执行一次
	err = clu.EveryHour(ctx, "hour_test_hour3", 10, 45, callback, "hour 这是小时任务3")
	fmt.Println(err)
	// 每小时的第15分钟0s执行一次
	err = clu.EveryHour(ctx, "hour_test_hour4", 15, 0, callback, "hour 这是小时任务4")
	fmt.Println(err)
	// 每小时的第20分钟15s执行一次
	err = clu.EveryHour(ctx, "hour_test_hour5", 20, 15, callback, "hour 这是小时任务5")
	fmt.Println(err)
}

// Day
func day(ctx context.Context, clu *timerx.Cluster) {
	// 每天的00:00:00执行一次
	err := clu.EveryDay(ctx, "day_test_day1", 0, 0, 0, callback, "day 这是天任务1")
	fmt.Println(err)
	// 每天的02:00:00执行一次
	err = clu.EveryDay(ctx, "day_test_day2", 2, 0, 0, callback, "day 这是天任务2")
	fmt.Println(err)
	// 每天的04:00:00执行一次
	err = clu.EveryDay(ctx, "day_test_day3", 4, 0, 0, callback, "day 这是天任务3")
	fmt.Println(err)
	// 每天的06:00:00执行一次
	err = clu.EveryDay(ctx, "day_test_day4", 6, 0, 0, callback, "day 这是天任务4")
	fmt.Println(err)
	// 每天的08:00:00执行一次
	err = clu.EveryDay(ctx, "day_test_day5", 8, 0, 0, callback, "day 这是天任务5")
	fmt.Println(err)
	// 每天的10:00:00执行一次
	err = clu.EveryDay(ctx, "day_test_day6", 10, 0, 0, callback, "day 这是天任务6")
	fmt.Println(err)
	// 每天的12:00:00执行一次
	err = clu.EveryDay(ctx, "day_test_day7", 12, 0, 0, callback, "day 这是天任务7")
	fmt.Println(err)
	// 每天的14:00:00执行一次
	err = clu.EveryDay(ctx, "day_test_day8", 14, 0, 0, callback, "day 这是天任务8")
	fmt.Println(err)
	// 每天的16:00:00执行一次
	err = clu.EveryDay(ctx, "day_test_day9", 16, 0, 0, callback, "day 这是天任务9")
	fmt.Println(err)
	// 每天的18:00:00执行一次
	err = clu.EveryDay(ctx, "day_test_day10", 18, 0, 0, callback, "day 这是天任务10")
	fmt.Println(err)
	// 每天的20:00:00执行一次
	err = clu.EveryDay(ctx, "day_test_day11", 20, 0, 0, callback, "day 这是天任务11")
	fmt.Println(err)
	// 每天的22:00:00执行一次
	err = clu.EveryDay(ctx, "day_test_day12", 22, 0, 0, callback, "day 这是天任务12")
	fmt.Println(err)
}

// Week
func week(ctx context.Context, clu *timerx.Cluster) {
	// 每周一 10:00:00 执行
	err := clu.EveryWeek(ctx, "week_test_week1", 1, 10, 0, 0, callback, "week 这是周任务1")
	fmt.Println(err)
	// 每周二 10:00:00 执行
	err = clu.EveryWeek(ctx, "week_test_week2", 2, 10, 0, 0, callback, "week 这是周任务2")
	fmt.Println(err)
	// 每周三 10:00:00 执行
	err = clu.EveryWeek(ctx, "week_test_week3", 3, 10, 0, 0, callback, "week 这是周任务3")
	fmt.Println(err)
	// 每周四 10:00:00 执行
	err = clu.EveryWeek(ctx, "week_test_week4", 4, 10, 0, 0, callback, "week 这是周任务4")
	fmt.Println(err)
	// 每周五 10:00:00 执行
	err = clu.EveryWeek(ctx, "week_test_week5", 5, 10, 0, 0, callback, "week 这是周任务5")
	fmt.Println(err)
	// 每周六 10:00:00 执行
	err = clu.EveryWeek(ctx, "week_test_week6", 6, 10, 0, 0, callback, "week 这是周任务6")
	fmt.Println(err)
	// 每周日 10:00:00 执行
	err = clu.EveryWeek(ctx, "week_test_week7", 0, 10, 0, 0, callback, "week 这是周任务7")
	fmt.Println(err)
}

// Month
func month(ctx context.Context, clu *timerx.Cluster) {
	// 每月的第1号 10:00:00 执行
	err := clu.EveryMonth(ctx, "month_test_month1", 1, 10, 0, 0, callback, "month 这是月任务1")
	fmt.Println(err)
	// 每月的第5号 10:00:00 执行
	err = clu.EveryMonth(ctx, "month_test_month5", 5, 10, 0, 0, callback, "month 这是月任务5")
	fmt.Println(err)
	// 每月的第10号 10:00:00 执行
	err = clu.EveryMonth(ctx, "month_test_month10", 10, 10, 0, 0, callback, "month 这是月任务10")
	fmt.Println(err)
	// 每月的第15号 10:00:00 执行
	err = clu.EveryMonth(ctx, "month_test_month15", 15, 10, 0, 0, callback, "month 这是月任务15")
	fmt.Println(err)
	// 每月的第20号 10:00:00 执行
	err = clu.EveryMonth(ctx, "month_test_month20", 20, 10, 0, 0, callback, "month 这是月任务20")
	fmt.Println(err)
	// 每月的第25号 10:00:00 执行
	err = clu.EveryMonth(ctx, "month_test_month25", 25, 10, 0, 0, callback, "month 这是月任务25")
	fmt.Println(err)
	// 每月的第28号 10:00:00 执行
	err = clu.EveryMonth(ctx, "month_test_month28", 28, 10, 0, 0, callback, "month 这是月任务28")
	fmt.Println(err)
	// 每月的第29号 10:00:00 执行
	err = clu.EveryMonth(ctx, "month_test_month29", 29, 10, 0, 0, callback, "month 这是月任务29")
	fmt.Println(err)
	// 每月的第30号 10:00:00 执行
	err = clu.EveryMonth(ctx, "month_test_month30", 30, 10, 0, 0, callback, "month 这是月任务30")
	fmt.Println(err)
	// 每月的第31号 10:00:00 执行
	err = clu.EveryMonth(ctx, "month_test_month31", 31, 10, 0, 0, callback, "month 这是月任务31")
	fmt.Println(err)

}

func cron(ctx context.Context, clu *timerx.Cluster) {
	// 秒级表达式 5秒执行一次
	err := clu.Cron(ctx, "cron_test_cron1", "*/5 * * * * ?", callback, "cron 这是cron任务1", timerx.WithCronParserSecond())
	fmt.Println(err)
	// Linux表达式 5分钟执行一次
	err = clu.Cron(ctx, "cron_test_cron2", "*/5 * * * *", callback, "cron 这是cron任务2", timerx.WithCronParserLinux())
	fmt.Println(err)
	// 符号表达式 5秒执行一次
	err = clu.Cron(ctx, "cron_test_cron3", "@every 5s", callback, "cron 这是cron任务3", timerx.WithCronParserDescriptor())
	fmt.Println(err)
	// 符号表达式 每天执行一次
	err = clu.Cron(ctx, "cron_test_cron4", "@daily", callback, "cron 这是cron任务4", timerx.WithCronParserDescriptor())
	fmt.Println(err)
	// 符号表达式 每月执行一次
	err = clu.Cron(ctx, "cron_test_cron5", "@monthly", callback, "cron 这是cron任务5", timerx.WithCronParserDescriptor())
	fmt.Println(err)
	// 符号表达式 每年执行一次
	err = clu.Cron(ctx, "cron_test_cron6", "@yearly", callback, "cron 这是cron任务6", timerx.WithCronParserDescriptor())
	fmt.Println(err)
	// 符号表达式 每周执行一次
	err = clu.Cron(ctx, "cron_test_cron7", "@weekly", callback, "cron 这是cron任务7", timerx.WithCronParserDescriptor())
	fmt.Println(err)
	// 符号表达式 每小时执行一次
	err = clu.Cron(ctx, "cron_test_cron8", "@hourly", callback, "cron 这是cron任务8", timerx.WithCronParserDescriptor())
	fmt.Println(err)
	// 符号表达式 每分钟执行一次
	err = clu.Cron(ctx, "cron_test_cron9", "@minutely", callback, "cron 这是cron任务9", timerx.WithCronParserDescriptor())
	fmt.Println(err)

}

func getRedis() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1" + ":" + "6379",
		Password: "123456", // no password set
		DB:       0,        // use default DB
	})
	if client == nil {
		panic("redis init error")
	}
	return client
}

func callback(ctx context.Context, extendData any) error {

	fmt.Println("任务执行了", extendData, "时间:", time.Now().Format("2006-01-02 15:04:05"))

	// 追加到文件
	file, err := os.OpenFile(save_path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("打开文件失败:", err)
		return err
	}
	defer file.Close()
	_, err = file.WriteString(fmt.Sprintf("执行时间:%v %s\n", extendData, time.Now().Format("2006-01-02 15:04:05")))
	if err != nil {
		fmt.Println("写入文件失败:", err)
		return err
	}

	return nil
}
