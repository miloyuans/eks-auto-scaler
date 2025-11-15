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
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cloudwatchtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"gopkg.in/yaml.v3"
)

// ==================== 配置结构 ====================
type NodeGroupConfig struct {
	Name     string   `yaml:"name"`
	Fallback []string `yaml:"fallback"` // 失败时尝试的备用组
}

type ClusterConfig struct {
	Name       string             `yaml:"name"`
	Region     string             `yaml:"region"`
	NodeGroups []NodeGroupConfig  `yaml:"nodegroups"`
}

type Config struct {
	Monitor struct {
		IntervalSeconds        int     `yaml:"interval_seconds"`
		MetricThresholdPercent float64 `yaml:"metric_threshold_percent"`
		ScaleUpBy              int     `yaml:"scale_up_by"`
	} `yaml:"monitor"`
	Telegram struct {
		Enabled  bool  `yaml:"enabled"`
		BotToken string `yaml:"bot_token"`
		ChatID   int64 `yaml:"chat_id"`
	} `yaml:"telegram"`
	CooldownMinutes int `yaml:"cooldown_minutes"`
	Clusters        []ClusterConfig `yaml:"clusters"`
}

// ==================== 客户端缓存 ====================
type Clients struct {
	eksClient   *eks.Client
	cwClient    *cloudwatch.Client
	asgClient   *autoscaling.Client
}

// ==================== Telegram 通知 ====================
type Notifier struct {
	bot     *tgbotapi.BotAPI
	chatID  int64
	enabled bool
}

func NewNotifier(token string, chatID int64, enabled bool) (*Notifier, error) {
	if !enabled {
		return &Notifier{enabled: false}, nil
	}
	bot, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		return nil, err
	}
	return &Notifier{bot: bot, chatID: chatID, enabled: true}, nil
}

func (n *Notifier) Send(msg string) {
	if !n.enabled {
		log.Printf("[Telegram Disabled] %s", truncate(msg, 100))
		return
	}
	message := tgbotapi.NewMessage(n.chatID, msg)
	message.ParseMode = "HTML"
	_, err := n.bot.Send(message)
	if err != nil {
		log.Printf("Telegram 发送失败: %v", err)
	} else {
		log.Printf("Telegram 已通知: %s", truncate(msg, 100))
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// ==================== 主结构体 ====================
type Scaler struct {
	cfg         Config
	notifier    *Notifier
	clients     map[string]*Clients // region -> clients
	scalingLock sync.Mutex
	currentTime time.Time
}

var globalScaler *Scaler

// ==================== 主函数 ====================
func main() {
	ctx := context.Background()

	// 加载配置
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatalf("读取 config.yaml 失败: %v", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("解析 config.yaml 失败: %v", err)
	}

	// 初始化 Telegram
	notifier, err := NewNotifier(cfg.Telegram.BotToken, cfg.Telegram.ChatID, cfg.Telegram.Enabled)
	if err != nil {
		log.Fatalf("初始化 Telegram 失败: %v", err)
	}

	// 初始化客户端（按 Region 缓存）
	clients := make(map[string]*Clients)
	for _, cluster := range cfg.Clusters {
		if _, exists := clients[cluster.Region]; exists {
			continue
		}
		awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cluster.Region))
		if err != nil {
			log.Printf("加载 Region %s 配置失败: %v", cluster.Region, err)
			continue
		}
		clients[cluster.Region] = &Clients{
			eksClient: eks.NewFromConfig(awsCfg),
			cwClient:  cloudwatch.NewFromConfig(awsCfg),
			asgClient: autoscaling.NewFromConfig(awsCfg),
		}
	}

	scaler := &Scaler{
		cfg:      cfg,
		notifier: notifier,
		clients:  clients,
	}
	globalScaler = scaler

	go scaler.startMonitoring(ctx)
	select {}
}

// ==================== 监控主循环 ====================
func (s *Scaler) startMonitoring(ctx context.Context) {
	interval := time.Duration(s.cfg.Monitor.IntervalSeconds) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("EKS 自动扩容监控启动，监控 %d 个集群", len(s.cfg.Clusters))
	s.notifier.Send(fmt.Sprintf("*EKS 自动扩容已启动*\n监控 %d 个集群 | 阈值 %.1f%%", len(s.cfg.Clusters), s.cfg.Monitor.MetricThresholdPercent))

	for {
		s.currentTime = time.Now().UTC()
		log.Printf("=== 探测开始 [%s] ===", s.currentTime.Format("2006-01-02 15:04:05 UTC"))

		for _, cluster := range s.cfg.Clusters {
			if err := s.processCluster(ctx, cluster); err != nil {
				log.Printf("处理集群 %s 失败: %v", cluster.Name, err)
			}
		}

		<-ticker.C
	}
}

func (s *Scaler) processCluster(ctx context.Context, clusterCfg ClusterConfig) error {
	clients, ok := s.clients[clusterCfg.Region]
	if !ok {
		return fmt.Errorf("Region %s 无客户端", clusterCfg.Region)
	}

	// 验证集群存在
	desc, err := clients.eksClient.DescribeCluster(ctx, &eks.DescribeClusterInput{Name: &clusterCfg.Name})
	if err != nil || desc.Cluster == nil {
		log.Printf("集群 %s 不存在或无法访问: %v", clusterCfg.Name, err)
		return nil
	}

	for _, ngCfg := range clusterCfg.NodeGroups {
		if err := s.checkAndScaleNodeGroup(ctx, clusterCfg, ngCfg, clients); err != nil {
			log.Printf("检查 NodeGroup %s 失败: %v", ngCfg.Name, err)
		}
	}
	return nil
}

// ==================== 检查并扩容 NodeGroup ====================
func (s *Scaler) checkAndScaleNodeGroup(ctx context.Context, clusterCfg ClusterConfig, ngCfg NodeGroupConfig, clients *Clients) error {
	// 获取 NodeGroup
	ng, err := clients.eksClient.DescribeNodegroup(ctx, &eks.DescribeNodegroupInput{
		ClusterName:   &clusterCfg.Name,
		NodegroupName: &ngCfg.Name,
	})
	if err != nil || ng.Nodegroup == nil {
		log.Printf("NodeGroup %s 不存在: %v", ngCfg.Name, err)
		return nil
	}
	if ng.Nodegroup.Resources == nil || len(ng.Nodegroup.Resources.AutoScalingGroups) == 0 {
		return fmt.Errorf("NodeGroup %s 无 ASG", ngCfg.Name)
	}
	asgName := *ng.Nodegroup.Resources.AutoScalingGroups[0].Name

	// 获取实例
	instances, err := s.getInstancesFromASG(ctx, clients.asgClient, asgName)
	if err != nil {
		return err
	}

	// 检查内存
	high := false
	var triggerInst string
	var triggerPct float64

	for _, inst := range instances {
		pct, err := s.getMemoryUsage(ctx, clients.cwClient, inst)
		if err != nil {
			log.Printf("  [Node %s] 获取内存失败: %v", inst, err)
			continue
		}
		log.Printf("  [Cluster: %s] [NodeGroup: %s] [Node %s] 内存: %.1f%%", clusterCfg.Name, ngCfg.Name, inst, pct)

		if pct >= s.cfg.Monitor.MetricThresholdPercent && !high {
			high = true
			triggerInst = inst
			triggerPct = pct
		}
	}

	if !high {
		return nil
	}

	// 触发扩容（带 fallback）
	candidates := append([]string{ngCfg.Name}, ngCfg.Fallback...)
	success := false
	var failReasons []string

	for _, name := range candidates {
		if success {
			break
		}
		log.Printf("尝试扩容 NodeGroup: %s", name)
		if err := s.scaleUpNodeGroup(ctx, clusterCfg, name, clients, triggerInst, triggerPct); err != nil {
			failReasons = append(failReasons, fmt.Sprintf("%s: %v", name, err))
			log.Printf("扩容 %s 失败: %v", name, err)
		} else {
			success = true
			log.Printf("扩容 %s 成功", name)
		}
	}

	if !success {
		errMsg := fmt.Sprintf(
			"*EKS 扩容失败*\n"+
				"集群: `%s`\n"+
				"触发节点: `%s` (%.1f%%)\n"+
				"失败原因:\n%s",
			clusterCfg.Name, triggerInst, triggerPct, "\n- "+strings.Join(failReasons, "\n- "),
		)
		s.notifier.Send(errMsg)
	}

	return nil
}

// ==================== 执行扩容 ====================
func (s *Scaler) scaleUpNodeGroup(ctx context.Context, clusterCfg ClusterConfig, ngName string, clients *Clients, inst string, pct float64) error {
	s.scalingLock.Lock()
	defer s.scalingLock.Unlock()

	// 获取 NodeGroup ASG
	ng, err := clients.eksClient.DescribeNodegroup(ctx, &eks.DescribeNodegroupInput{
		ClusterName:   &clusterCfg.Name,
		NodegroupName: &ngName,
	})
	if err != nil || ng.Nodegroup == nil {
		return fmt.Errorf("NodeGroup %s 不存在", ngName)
	}
	if ng.Nodegroup.Resources == nil || len(ng.Nodegroup.Resources.AutoScalingGroups) == 0 {
		return fmt.Errorf("NodeGroup %s 无 ASG", ngName)
	}
	asgName := *ng.Nodegroup.Resources.AutoScalingGroups[0].Name

	// 冷却检查
	if s.isInCooldown(ctx, clients.asgClient, asgName) {
		return fmt.Errorf("ASG %s 冷却中", asgName)
	}

	// 获取当前 Desired
	desc, err := clients.asgClient.DescribeAutoScalingGroups(ctx, &autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{asgName},
	})
	if err != nil || len(desc.AutoScalingGroups) == 0 {
		return fmt.Errorf("获取 ASG 失败")
	}
	current := *desc.AutoScalingGroups[0].DesiredCapacity
	newDesired := current + int32(s.cfg.Monitor.ScaleUpBy)

	// 执行扩容
	_, err = clients.asgClient.UpdateAutoScalingGroup(ctx, &autoscaling.UpdateAutoScalingGroupInput{
		AutoScalingGroupName: &asgName,
		DesiredCapacity:      &newDesired,
	})
	if err != nil {
		return err
	}

	// 添加冷却标签
	s.addCooldownTag(ctx, clients.asgClient, asgName)

	// 成功通知
	successMsg := fmt.Sprintf(
		"*EKS 自动扩容成功*\n"+
			"集群: `%s`\n"+
			"NodeGroup: `%s`\n"+
			"触发节点: `%s` (%.1f%%)\n"+
			"ASG: `%s`\n"+
			"Desired: `%d to %d`\n"+
			"时间: `%s`",
		clusterCfg.Name, ngName, inst, pct, asgName, current, newDesired, s.currentTime.Format("15:04:05 UTC"),
	)
	log.Println(successMsg)
	s.notifier.Send(successMsg)

	return nil
}

// ==================== 工具函数 ====================
func (s *Scaler) getInstancesFromASG(ctx context.Context, client *autoscaling.Client, asgName string) ([]string, error) {
	resp, err := client.DescribeAutoScalingGroups(ctx, &autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{asgName},
	})
	if err != nil || len(resp.AutoScalingGroups) == 0 {
		return nil, fmt.Errorf("ASG %s 无实例", asgName)
	}
	var instances []string
	for _, inst := range resp.AutoScalingGroups[0].Instances {
		if inst.InstanceId != nil {
			instances = append(instances, *inst.InstanceId)
		}
	}
	return instances, nil
}

func (s *Scaler) getMemoryUsage(ctx context.Context, client *cloudwatch.Client, inst string) (float64, error) {
	end := s.currentTime
	start := end.Add(-6 * time.Minute)

	input := &cloudwatch.GetMetricStatisticsInput{
		Namespace:  aws.String("ContainerInsights"),
		MetricName: aws.String("mem_used_percent"),
		Dimensions: []cloudwatchtypes.Dimension{
			{Name: aws.String("InstanceId"), Value: aws.String(inst)},
		},
		StartTime:  &start,
		EndTime:    &end,
		Period:     aws.Int32(300),
		Statistics: []cloudwatchtypes.Statistic{cloudwatchtypes.StatisticAverage},
	}

	out, err := client.GetMetricStatistics(ctx, input)
	if err != nil || len(out.Datapoints) == 0 {
		return 0, fmt.Errorf("无数据")
	}

	var max float64
	for _, dp := range out.Datapoints {
		if dp.Average != nil && *dp.Average > max {
			max = *dp.Average
		}
	}
	return math.Round(max*10) / 10, nil
}

func (s *Scaler) isInCooldown(ctx context.Context, client *autoscaling.Client, asg string) bool {
	out, err := client.DescribeTags(ctx, &autoscaling.DescribeTagsInput{
		Filters: []types.Filter{
			{Name: aws.String("auto-scaling-group"), Values: []string{asg}},
			{Name: aws.String("key"), Values: []string{"eks-auto-scaled-at"}},
		},
	})
	if err != nil || len(out.Tags) == 0 {
		return false
	}
	for _, t := range out.Tags {
		if *t.Key == "eks-auto-scaled-at" {
			ts, _ := time.Parse(time.RFC3339, *t.Value)
			return s.currentTime.Sub(ts) < time.Duration(s.cfg.CooldownMinutes)*time.Minute
		}
	}
	return false
}

func (s *Scaler) addCooldownTag(ctx context.Context, client *autoscaling.Client, asg string) {
	ts := s.currentTime.Format(time.RFC3339)
	_, _ = client.CreateOrUpdateTags(ctx, &autoscaling.CreateOrUpdateTagsInput{
		Tags: []types.Tag{
			{
				ResourceId:        aws.String(asg),
				ResourceType:      aws.String("auto-scaling-group"),
				Key:               aws.String("eks-auto-scaled-at"),
				Value:             aws.String(ts),
				PropagateAtLaunch: aws.Bool(false),
			},
		},
	})
}