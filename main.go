// main.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"
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
	"github.com/aws/aws-sdk-go-v2/service/ec2" // 用于解析 IID
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"gopkg.in/yaml.v3"
)

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

var globalScaler *Scaler

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

	// AWS 配置
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("加载 AWS 配置失败: %v", err)
	}

	// 自动获取 Region（使用 IMDS）
	awsCfg.Region = getRegion(ctx)

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
	}
	globalScaler = scaler

	// 异步启动监控
	go scaler.startMonitoring(ctx)

	// 保持运行
	select {}
}

func getRegion(ctx context.Context) string {
	imdsClient := imds.New(imds.Options{})
	iid, err := imdsClient.GetMetadata(ctx, &imds.GetMetadataInput{
		Path: aws.String("/latest/dynamic/instance-identity/document"),
	})
	if err != nil {
		log.Printf("获取 IMDS 失败，使用默认 Region: %v", err)
		return "us-east-1"
	}

	var doc struct {
		Region string `json:"region"`
	}
	if err := json.Unmarshal([]byte(iid.Content), &doc); err != nil {
		log.Printf("解析 IID 失败: %v", err)
		return "us-east-1"
	}
	return doc.Region
}

func (s *Scaler) startMonitoring(ctx context.Context) {
	interval := time.Duration(s.cfg.Monitor.IntervalSeconds) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("监控已启动: 间隔 %ds | 阈值 %.1f%% | 扩容 +%d", s.cfg.Monitor.IntervalSeconds, s.cfg.Monitor.MetricThresholdPercent, s.cfg.Monitor.ScaleUpBy)
	s.notifier.Send(fmt.Sprintf("EKS 自动扩容监控已启动\n间隔: %ds | 阈值: %.1f%% | 扩容: +%d", s.cfg.Monitor.IntervalSeconds, s.cfg.Monitor.MetricThresholdPercent, s.cfg.Monitor.ScaleUpBy))

	for {
		s.currentTime = time.Now().UTC()
		log.Printf("=== 开始探测 [%s] ===", s.currentTime.Format("2006-01-02 15:04:05 UTC"))

		if err := s.runOnce(ctx); err != nil {
			log.Printf("监控周期异常: %v", err)
			s.notifier.Send(fmt.Sprintf("监控周期异常: %v", err))
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
	p := eks.NewListClustersPaginator(s.eksClient, &eks.ListClustersInput{})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		clusters = append(clusters, page.Clusters...)
	}
	return clusters, nil
}

func (s *Scaler) processCluster(ctx context.Context, cluster string) error {
	ngs, err := s.listNodegroups(ctx, cluster)
	if err != nil {
		return err
	}
	for _, ng := range ngs {
		if err := s.checkNodeGroup(ctx, cluster, ng); err != nil {
			log.Printf("检查 NodeGroup %s 失败: %v", *ng.NodegroupName, err)
		}
	}
	return nil
}

func (s *Scaler) listNodegroups(ctx context.Context, cluster string) ([]ekstypes.Nodegroup, error) {
	var ngs []ekstypes.Nodegroup
	p := eks.NewListNodegroupsPaginator(s.eksClient, &eks.ListNodegroupsInput{ClusterName: &cluster})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, name := range page.Nodegroups {
			desc, err := s.eksClient.DescribeNodegroup(ctx, &eks.DescribeNodegroupInput{
				ClusterName:   &cluster,
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

func (s *Scaler) checkNodeGroup(ctx context.Context, cluster string, ng ekstypes.Nodegroup) error {
	if ng.Resources == nil || len(ng.Resources.AutoScalingGroups) == 0 {
		return fmt.Errorf("无 ASG")
	}
	asgName := *ng.Resources.AutoScalingGroups[0].Name
	instances, err := s.getNodeInstances(ctx, cluster, ng)
	if err != nil {
		return err
	}

	high := false
	var triggerInst string
	var triggerPct float64

	for _, inst := range instances {
		pct, err := s.getMemoryUsage(ctx, inst)
		if err != nil {
			log.Printf("  [Node %s] 获取失败: %v", inst, err)
			continue
		}
		log.Printf("  [Node %s] 内存平均: %.1f%%", inst, pct)

		if pct >= s.cfg.Monitor.MetricThresholdPercent && !high {
			high = true
			triggerInst = inst
			triggerPct = pct
		}
	}

	if high {
		go s.triggerScaleUp(ctx, cluster, *ng.NodegroupName, asgName, triggerInst, triggerPct)
	}
	return nil
}

func (s *Scaler) triggerScaleUp(ctx context.Context, cluster, ngName, asgName, inst string, pct float64) {
	s.scalingLock.Lock()
	defer s.scalingLock.Unlock()

	log.Printf("高负载触发: %s (%.1f%%) → 扩容 %s (ASG: %s)", inst, pct, ngName, asgName)

	if s.isInCooldown(ctx, asgName) {
		msg := fmt.Sprintf("ASG %s 冷却中，跳过", asgName)
		log.Println(msg)
		s.notifier.Send(msg)
		return
	}

	desc, err := s.asgClient.DescribeAutoScalingGroups(ctx, &autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{asgName},
	})
	if err != nil || len(desc.AutoScalingGroups) == 0 {
		s.notifier.Send(fmt.Sprintf("获取 ASG 失败: %v", err))
		return
	}
	current := *desc.AutoScalingGroups[0].DesiredCapacity
	newDesired := current + int32(s.cfg.Monitor.ScaleUpBy)

	log.Printf("执行扩容: %s Desired %d → %d", asgName, current, newDesired)

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

	s.addCooldownTag(ctx, asgName)

	successMsg := fmt.Sprintf(
		"*EKS 自动扩容成功*\n"+
			"集群: `%s`\n"+
			"NodeGroup: `%s`\n"+
			"触发节点: `%s` (%.1f%%)\n"+
			"ASG: `%s`\n"+
			"Desired: `%d → %d`\n"+
			"时间: `%s`",
		cluster, ngName, inst, pct, asgName, current, newDesired, s.currentTime.Format("15:04:05 UTC"),
	)
	log.Println(successMsg)
	s.notifier.Send(successMsg)
}

func (s *Scaler) getNodeInstances(ctx context.Context, cluster string, ng ekstypes.Nodegroup) ([]string, error) {
	var insts []string
	p := eks.NewListPodsPaginator(s.eksClient, &eks.ListPodsInput{ // 修正：ListNodes 使用 ListPods
		ClusterName: &cluster,
		NodegroupName: ng.NodegroupName,
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, node := range page.Pods { // 修正：Pods 字段
			if len(*node) >= 19 && strings.HasPrefix(*node, "i-") {
				insts = append(insts, (*node)[:19])
			}
		}
	}
	return insts, nil
}

func (s *Scaler) getMemoryUsage(ctx context.Context, inst string) (float64, error) {
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

	out, err := s.cwClient.GetMetricStatistics(ctx, input)
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

func (s *Scaler) isInCooldown(ctx context.Context, asg string) bool {
	out, err := s.asgClient.DescribeTags(ctx, &autoscaling.DescribeTagsInput{
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

func (s *Scaler) addCooldownTag(ctx context.Context, asg string) {
	ts := s.currentTime.Format(time.RFC3339)
	_, _ = s.asgClient.CreateOrUpdateTags(ctx, &autoscaling.CreateOrUpdateTagsInput{
		Tags: []types.Tag{
			{
				ResourceId:        aws.String(asg),
				ResourceType:      aws.String("auto-scaling-group"),
				Key:               aws.String("eks-auto-scaled-at"),
				Value:             &ts,
				PropagateAtLaunch: aws.Bool(false),
			},
		},
	})
}