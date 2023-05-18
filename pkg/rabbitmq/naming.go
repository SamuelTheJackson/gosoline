package rabbitmq

import (
	"fmt"
	"strings"

	"github.com/justtrackio/gosoline/pkg/cfg"
)

const FifoSuffix = "fifo"

type NameSettingAware interface {
	GetAppId() cfg.AppId
	GetClientName() string
	GetQueueId() string
	IsFifoEnabled() bool
	GetExchangeId() string
}

type QueueNameSettings struct {
	AppId       cfg.AppId
	ClientName  string
	FifoEnabled bool
	QueueId     string
	ExchangeId  string
}

func (s QueueNameSettings) GetAppId() cfg.AppId {
	return s.AppId
}

func (s QueueNameSettings) GetClientName() string {
	return s.ClientName
}

func (s QueueNameSettings) IsFifoEnabled() bool {
	return s.FifoEnabled
}

func (s QueueNameSettings) GetQueueId() string {
	return s.QueueId
}

func (s QueueNameSettings) GetExchangeId() string {
	return s.ExchangeId
}

type QueueNameSetting struct {
	Patter string `cfg:"pattern,nodecode" default:"{project}-{env}-{family}-{app}-{queueId}"`
}

type ExchangeNameSetting struct {
	Patter string `cfg:"pattern,nodecode" default:"{project}-{env}-{family}-{app}-{exchangeId}"`
}

func GetQueueSettings(config cfg.Config, queueSettings NameSettingAware) (*QueueSettings, error) {
	if len(queueSettings.GetClientName()) == 0 {
		return nil, fmt.Errorf("the client name shouldn't be empty")
	}

	namingKey := fmt.Sprintf("rabbitmq.%s.naming", queueSettings.GetClientName())
	namingSettings := &QueueNameSetting{}
	config.UnmarshalKey(namingKey, namingSettings)

	name := namingSettings.Patter
	appId := queueSettings.GetAppId()
	values := map[string]string{
		"project": appId.Project,
		"env":     appId.Environment,
		"family":  appId.Family,
		"app":     appId.Application,
		"queueId": queueSettings.GetQueueId(),
	}

	for key, val := range values {
		templ := fmt.Sprintf("{%s}", key)
		name = strings.ReplaceAll(name, templ, val)
	}

	return &QueueSettings{Name: name}, nil
}

func GetExchangeName(config cfg.Config, exchangeSetting NameSettingAware) (*ExchangeSettings, error) {
	if len(exchangeSetting.GetClientName()) == 0 {
		return nil, fmt.Errorf("the client name shouldn't be empty")
	}

	namingKey := fmt.Sprintf("rabbitmq.exchange.%s.naming", exchangeSetting.GetClientName())
	namingSettings := &ExchangeNameSetting{}
	config.UnmarshalKey(namingKey, namingSettings)

	name := namingSettings.Patter
	appId := exchangeSetting.GetAppId()
	values := map[string]string{
		"project":    appId.Project,
		"env":        appId.Environment,
		"family":     appId.Family,
		"app":        appId.Application,
		"exchangeId": exchangeSetting.GetExchangeId(),
	}

	for key, val := range values {
		templ := fmt.Sprintf("{%s}", key)
		name = strings.ReplaceAll(name, templ, val)
	}

	return &ExchangeSettings{
		Name: name,
		Type: "",
	}, nil
}
