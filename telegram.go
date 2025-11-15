// telegram.go
package main

import (
	"log"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

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