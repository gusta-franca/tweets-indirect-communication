/*
   Data de criação: 14/11/2026
   Estudante: Gustavo Martins França
   Tratamento dos pedidos de assinatura na fila de controle
*/

package main

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Processa um pedido da fila de controle
func handleDelivery(ch *amqp.Channel, d amqp.Delivery, topics map[string]bool) error {
	var reply Reply

	cmd, err := parseCommand(d.Body, topics)

	if err != nil {
		log.Printf("pedido rejeitado: %v", err)
		reply = Reply{OK: false, Error: err.Error()}
	} else {
		if err := apply(ch, cmd); err != nil {
			return err
		}

		log.Printf("%s: %s -> %v", cmd.Action, subscriberQueue(cmd.Subscriber), cmd.Topics)
		reply = Reply{OK: true, Subscriber: cmd.Subscriber, Queue: subscriberQueue(cmd.Subscriber), Topics: cmd.Topics}
	}

	if d.ReplyTo != "" {
		body, _ := json.Marshal(reply)
		err := ch.Publish(
			"",
			d.ReplyTo,
			false,
			false,
			amqp.Publishing{
				ContentType:   "application/json",
				CorrelationId: d.CorrelationId,
				Body:          body,
			})

		if err != nil {
			return err
		}
	}

	return d.Ack(false)
}
