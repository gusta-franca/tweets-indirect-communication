/*
   Data de criação: 14/11/2026
   Estudante: Gustavo Martins França
   Definição da assinatura
*/

package main

import (
	"encoding/json"
	"fmt"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Command é o pedido que um assinante publica em tweets.control
// ex.
// {"action":"subscribe","subscriber":"alice","topics":["futebol","voleibol"]}
// {"action":"unsubscribe","subscriber":"alice","topics":["voleibol"]}
type Command struct {
	Action     string   `json:"action"`
	Subscriber string   `json:"subscriber"`
	Topics     []string `json:"topics"`
}

// Reply é a resposta enviada para a fila indicada em reply_to.
type Reply struct {
	OK         bool     `json:"ok"`
	Error      string   `json:"error,omitempty"`
	Subscriber string   `json:"subscriber,omitempty"`
	Queue      string   `json:"queue,omitempty"`
	Topics     []string `json:"topics,omitempty"`
}

// O AMQP limita nomes (fila, routing key) a 255 bytes
const maxNameLen = 255

// A fila do assinante é subQueuePrefix + nome, então o prefixo também conta no limite
const maxSubscriberLen = maxNameLen - len(subQueuePrefix)

// Executa o pedido no broker
func apply(ch *amqp.Channel, cmd Command) error {
	q := subscriberQueue(cmd.Subscriber)

	if _, err := ch.QueueDeclare(q, true, false, false, false, nil); err != nil {
		return err
	}

	for _, t := range cmd.Topics {
		var err error

		if cmd.Action == "subscribe" {
			err = ch.QueueBind(q, t, topicsExchange, false, nil)
		} else {
			err = ch.QueueUnbind(q, t, topicsExchange, nil)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

// Valida e normaliza um pedido de inscrição
func parseCommand(body []byte, allowed map[string]bool) (Command, error) {
	var cmd Command

	if err := json.Unmarshal(body, &cmd); err != nil {
		return cmd, fmt.Errorf("Invalid JSON: %v", err)
	}

	cmd.Action = strings.ToLower(strings.TrimSpace(cmd.Action))

	if cmd.Action != "subscribe" && cmd.Action != "unsubscribe" {
		return cmd, fmt.Errorf(`Action must be "subscribe" or "unsubscribe"`)
	}

	cmd.Subscriber = strings.ToLower(strings.TrimSpace(cmd.Subscriber))

	if cmd.Subscriber == "" || len(cmd.Subscriber) > maxSubscriberLen {
		return cmd, fmt.Errorf("Invalid subscriber (vazio ou maior que %d bytes)", maxSubscriberLen)
	}

	seen := map[string]bool{}
	var topics []string

	for _, t := range cmd.Topics {
		t = strings.ToLower(strings.TrimSpace(t))

		if t == "" || len(t) > maxNameLen {
			return cmd, fmt.Errorf("tópico inválido (vazio ou maior que %d bytes)", maxNameLen)
		}

		if allowed != nil && !allowed[t] {
			return cmd, fmt.Errorf("tópico desconhecido: %q", t)
		}

		if !seen[t] {
			seen[t] = true
			topics = append(topics, t)
		}
	}

	if len(topics) == 0 {
		return cmd, fmt.Errorf("topics não pode ser vazio")
	}

	cmd.Topics = topics
	return cmd, nil
}

// Helper para encontrar a fila de um assinante
func subscriberQueue(subscriber string) string {
	return subQueuePrefix + subscriber
}
