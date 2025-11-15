// go.mod
module eks-auto-scaler

go 1.25

require (
    github.com/aws/aws-sdk-go-v2 v1.30.3
    github.com/aws/aws-sdk-go-v2/config v1.27.10
    github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.11
    github.com/aws/aws-sdk-go-v2/service/autoscaling v1.42.0
    github.com/aws/aws-sdk-go-v2/service/cloudwatch v1.40.0
    github.com/aws/aws-sdk-go-v2/service/eks v1.46.0
    github.com/go-telegram-bot-api/telegram-bot-api/v5 v5.5.1
    gopkg.in/yaml.v3 v3.0.1
)