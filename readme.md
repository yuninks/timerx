# 功能支持

1. 支持本地任务
2. 支持集群任务
3. 支持单次任务

# 功能说明



# 功能实现

1. 集群间任务调度和任务的唯一依赖于redis进行实现


# 缺陷

1. 集群部署时，存在新旧的代码混合问题，任务调度可能存在问题（需要根据实际需要进行版本上线/下线操作）

