package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yuninks/timerx"
)

func main() {
	ctx := context.Background()

	client := getClusterRedis()

	clu, err := timerx.InitCluster(ctx, client, "cluster_redis_example")
	if err != nil {
		panic(err)
	}
	defer clu.Stop()

	err = clu.EverySpace(ctx, "cluster_every_second", 1*time.Second, clusterCallback, "cluster每秒任务")
	fmt.Println("EverySpace:", err)

	err = clu.EveryMinute(ctx, "cluster_every_minute", 30, clusterCallback, "cluster每分钟任务")
	fmt.Println("EveryMinute:", err)

	err = clu.EveryDay(ctx, "cluster_every_day", 0, 0, 0, clusterCallback, "cluster每天任务")
	fmt.Println("EveryDay:", err)

	err = clu.Cron(ctx, "cluster_cron_second", "*/5 * * * * ?", clusterCallback, "cluster cron任务",
		timerx.WithCronParserSecond())
	fmt.Println("Cron:", err)

	select {}
}

func getClusterRedis() *redis.ClusterClient {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{
			"127.0.0.1:6379",
			"127.0.0.1:6380",
			"127.0.0.1:6381",
		},
		Password: "",
	})
	if client == nil {
		panic("redis cluster init error")
	}
	return client
}

func clusterCallback(ctx context.Context, extendData any) error {
	fmt.Println("cluster任务执行:", extendData, "时间:", time.Now().Format("2006-01-02 15:04:05"))
	return nil
}
