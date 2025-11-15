// main.go
package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cloudwatchtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Monitor struct {
		IntervalSeconds       int     `yaml:"interval_seconds"`
		MetricThresholdPercent float64 `yaml:"metric_threshold_percent"`
		ScaleUpBy             int     `yaml:"scale_up_by"`
	} `yaml:"monitor"`
	Telegram struct {
		Enabled  bool   `yaml:"enabled"`
		BotToken string `yaml:"bot_token"`
		ChatID   int64  `yaml:"chat_id"`
	} `yaml:"telegram"`
	CooldownMinutes int `yaml:"cooldown_minutes"`
}

type Scaler struct {
	cfg           Config
	eksClient     *eks.Client
	cwClient      *cloudwatch.Client
	asgClient     *autoscaling.Client
	notifier      *Notifier
	scalingLock   sync.Mutex
	currentTime   time.Time
	region        string
}

var globalScaler *Scaler

func main() {
	ctx := context.Background()

	// 加载配置
	configData, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatalf("读取 config.yaml 失败: %v", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(configData, &cfg); err != nil {
		log.Fatalf("解析 config.yaml 失败: %v", err)
	}

	// AWS 配置
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("加载 AWS 配置失败: %v", err)
	}

	// 自动获取 Region
	imdsClient := imds.New(imds.Options{})
	inst, _ := imdsClient.GetInstanceInfo(ctx, &imds.GetInstanceInfoInput{})
	if inst != nil && inst.InstanceInfo.Region != nil {
		awsCfg.Region = *inst.InstanceInfo.Region
	}
	if awsCfg.Region == "" {
		awsCfg.Region = "us-east-1"
	}

	// 初始化 Notifier
	notifier, err := NewNotifier(cfg.Telegram.BotToken, cfg.Telegram.ChatID, cfg.Telegram.Enabled)
	if err != nil {
		log.Fatalf("初始化 Telegram 失败: %v", err)
	}

	scaler := &Scaler{
		cfg:         cfg,
		eksClient:   eks.NewFromConfig(awsCfg),
		cwClient:    cloudwatch.NewFromConfig(awsCfg),
		asgClient:   autoscaling.NewFromConfig(awsCfg),
		notifier:    notifier,
		region:      awsCfg.Region,
		currentTime: time.Now().UTC(),
	}
	globalScaler = scaler

	// 启动异步监控
	go scaler.startMonitoring(ctx)

	// 主线程保持运行
	select {}
}

func (s *Scaler) startMonitoring(ctx context.Context) {
	interval := time.Duration(s.cfg.Monitor.IntervalSeconds) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("监控启动: 每 %d 秒探测一次，阈值 %.1f%%，扩容 +%d", s.cfg.Monitor.IntervalSeconds, s.cfg.Monitor.MetricThresholdPercent, s.cfg.Monitor.ScaleUpBy)
	s.notifier.Send(fmt.Sprintf("EKS 自动扩容监控已启动\n间隔: %ds | 阈值: %.1f%% | 扩容: +%d", s.cfg.Monitor.IntervalSeconds, s.cfg.Monitor.MetricThresholdPercent, s.cfg.Monitor.ScaleUpBy))

	for {
		s.currentTime = time.Now().UTC()
		log.Printf("=== 开始第 %s 次监控探测 ===", s.currentTime.Format("2006-01-02 15:04:05 UTC"))

		if err := s.runOnce(ctx); err != nil {
			log.Printf("监控周期失败: %v", err)
			s.notifier.Send(fmt.Sprintf("监控周期失败: %v", err))
		}

		<-ticker.C
	}
}

func (s *Scaler) runOnce(ctx context.Context) error {
	clusters, err := s.listEKSClusters(ctx)
	if err != nil {
		return err
	}

	for _, cluster := range clusters {
		if err := s.processCluster(ctx, cluster); err != nil {
			log.Printf("处理集群 %s 失败: %v", cluster, err)
		}
	}
	return nil
}

func (s *Scaler) listEKSClusters(ctx context.Context) ([]string, error) {
	var clusters []string
	paginator := eks.NewListClustersPaginator(s.eksClient, &eks.ListClustersInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		clusters = append(clusters, page.Clusters...)
	}
	return clusters, nil
}

func (s *Scaler) processCluster(ctx context.Context, clusterName string) error {
	nodegroups, err := s.listNodegroups(ctx, clusterName)
	if err != nil {
		return err
	}

	for _, ng := range nodegroups {
		if err := s.checkNodeGroup(ctx, clusterName, ng); err != nil {
			log.Printf("检查 NodeGroup %s 失败: %v", *ng.NodegroupName, err)
		}
	}
	return nil
}

func (s *Scaler) listNodegroups(ctx context.Context, clusterName string) ([]ekstypes.Nodegroup, error) {
	var ngs []ekstypes.Nodegroup
	paginator := eks.NewListNodegroupsPaginator(s.eksClient, &eks.ListNodegroupsInput{ClusterName: &clusterName})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, name := range page.Nodegroups {
			desc, err := s.eksClient.DescribeNodegroup(ctx, &eks.DescribeNodegroupInput{
				ClusterName:   &clusterName,
				NodegroupName: &name,
			})
			if err != nil {
				continue
			}
			ngs = append(ngs, *desc.Nodegroup)
		}
	}
	return ngs, nil
}

func (s *Scaler) checkNodeGroup(ctx context.Context, clusterName string, ng ekstypes.Nodegroup) error {
	if ng.Resources == nil || len(ng.Resources.AutoScalingGroups) == 0 {
		return fmt.Errorf("无 ASG")
	}
	asgName := *ng.Resources.AutoScalingGroups[0].Name
	instances, err := s.getNodeInstances(ctx, clusterName, ng)
	if err != nil {
		return err
	}

	// 打印每个节点内存
	highUsage := false
	var highInstance string
	var highPercent float64

	for _, inst := range instances {
		usage, err := s.getMemoryUsage(ctx, inst)
		if err != nil {
			log.Printf("  [Node %s] 获取内存失败: %v", inst, err)
			continue
		}
		log.Printf("  [Node %s] 内存平均使用率: %.1f%%", inst, usage)

		if usage >= s.cfg.Monitor.MetricThresholdPercent && !highUsage {
			highUsage = true
			highInstance = inst
			highPercent = usage
		}
	}

	if !highUsage {
		return nil
	}

	// 触发扩容（串行）
	go s.triggerScaleUp(ctx, clusterName, *ng.NodegroupName, asgName, highInstance, highPercent, instances)
	return nil
}

func (s *Scaler) triggerScaleUp(ctx context.Context, cluster, ngName, asgName, instanceID string, percent float64, instances []string) {
	s.scalingLock.Lock()
	defer s.scalingLock.Unlock()

	log.Printf("检测到高负载节点: %s (%.1f%%) → 准备扩容 NodeGroup %s (ASG: %s)", instanceID, percent, ngName, asgName)

	// 冷却检查
	if s.isInCooldown(ctx, asgName) {
		msg := fmt.Sprintf("ASG %s 处于冷却期，跳过扩容", asgName)
		log.Println(msg)
		s.notifier.Send(msg)
		return
	}

	// 获取当前 Desired
	desc, err := s.asgClient.DescribeAutoScalingGroups(ctx, &autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{asgName},
	})
	if err != nil || len(desc.AutoScalingGroups) == 0 {
		s.notifier.Send(fmt.Sprintf("获取 ASG %s 失败: %v", asgName, err))
		return
	}
	current := *desc.AutoScalingGroups[0].DesiredCapacity
	newDesired := current + int32(s.cfg.Monitor.ScaleUpBy)

	log.Printf("开始扩容: %s Desired %d → %d", asgName, current, newDesired)

	_, err = s.asgClient.UpdateAutoScalingGroup(ctx, &autoscaling.UpdateAutoScalingGroupInput{
		AutoScalingGroupName: &asgName,
		DesiredCapacity:      &newDesired,
	})
	if err != nil {
		errMsg := fmt.Sprintf("扩容失败: %v", err)
		log.Println(errMsg)
		s.notifier.Send(errMsg)
		return
	}

	// 打冷却标签
	s.addCooldownTag(ctx, asgName)

	// 成功通知
	successMsg := fmt.Sprintf(
		"*EKS 自动扩容成功*\n"+
			"集群: `%s`\n"+
			"NodeGroup: `%s`\n"+
			"触发节点: `%s` (%.1f%%)\n"+
			"ASG: `%s`\n"+
			"Desired: `%d → %d`\n"+
			"时间: `%s`",
		cluster, ngName, instanceID, percent, asgName, current, newDesired, s.currentTime.Format("15:04:05 UTC"),
	)
	log.Println(successMsg)
	s.notifier.Send(successMsg)
}

func (s *Scaler) getNodeInstances(ctx context.Context, clusterName string, ng ekstypes.Nodegroup) ([]string, error) {
	var instances []string
	paginator := eks.NewListNodesPaginator(s.eksClient, &eks.ListNodesInput{
		ClusterName:   &clusterName,
		NodegroupName: ng.NodegroupName,
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, node := range page.Nodes {
			if len(node) >= 19 && node[:2] == "i-" {
				instances = append(instances, node[:19])
			}
		}
	}
	return instances, nil
}

func (s *Scaler) getMemoryUsage(ctx context.Context, instanceID string) (float64, error) {
	end := s.currentTime
	start := end.Add(-6 * time.Minute)

	input := &cloudwatch.GetMetricStatisticsInput{
		Namespace:  aws.String("ContainerInsights"),
		MetricName: aws.String("mem_used_percent"),
		Dimensions: []cloudwatchtypes.Dimension{
			{Name: aws.String("InstanceId"), Value: &instanceID},
		},
		StartTime:  &start,
		EndTime:    &end,
		Period:     aws.Int32(300),
		Statistics: []cloudwatchtypes.Statistic{cloudwatchtypes.StatisticAverage},
	}

	result, err := s.cwClient.GetMetricStatistics(ctx, input)
	if err != nil || len(result.Datapoints) == 0 {
		return 0, fmt.Errorf("无数据")
	}

	var max float64
	for _, dp := range result.Datapoints {
		if dp.Average != nil && *dp.Average > max {
			max = *dp.Average
		}
	}
	return math.Round(max*10) / 10, nil
}

func (s *Scaler) isInCooldown(ctx context.Context, asgName string) bool {
	output, err := s.asgClient.DescribeTags(ctx, &autoscaling.DescribeTagsInput{
		Filters: []types.Filter{
			{Name: aws.String("auto-scaling-group"), Values: []string{asgName}},
			{Name: aws.String("key"), Values: []string{"eks-auto-scaled-at"}},
		},
	})
	if err != nil || len(output.Tags) == 0 {
		return false
	}
	for _, tag := range output.Tags {
		if *tag.Key == "eks-auto-scaled-at" {
			t, _ := time.Parse(time.RFC3339, *tag.Value)
			return s.currentTime.Sub(t) < time.Duration(s.cfg.CooldownMinutes)*time.Minute
		}
	}
	return false
}

func (s *Scaler) addCooldownTag(ctx context.Context, asgName string) {
	timestamp := s.currentTime.Format(time.RFC3339)
	s.asgClient.CreateOrUpdateTags(ctx, &autoscaling.CreateOrUpdateTagsInput{
		Tags: []types.Tag{
			{
				ResourceId:        &asgName,
				ResourceType:      aws.String("auto-scaling-group"),
				Key:               aws.String("eks-auto-scaled-at"),
				Value:             &timestamp,
				PropagateAtLaunch: aws.Bool(false),
			},
		},
	})
}