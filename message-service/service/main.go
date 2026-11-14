/*
	Data de criação: 13/11/2026
	Estudante: Gustavo Martins França
	Potno de entrada do serviço de mensagens
*/

package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ingestExchange = "tweets.ingest" // coletor publica em ingestExchange
	rawQueue       = "tweets.raw"    // classificadores consomem rawQueue
	rawRoutingKey  = "raw"
	topicsExchange = "tweets.topics"  // classificador publica em topicsExchange
	controlQueue   = "tweets.control" // assinantes assinam via controlQueue
	subQueuePrefix = "sub."           // fila de cada assinante definida com subQueuePrefix
	prefetch       = 10
	retryInterval  = 3 * time.Second
)

func main() {
	url := loadEnv("AMQP_URL", "amqp://tweets:tweets@localhost:5672/")
	topics := loadTopics()
	logFile, err := startLogging(loadEnv("LOG_FILE", "logs/message-service.log"))

	if err != nil {
		log.Fatalf("Could not set up log file: %s", err)
	}

	defer logFile.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("Message service started (topics: %s)", printTopics(topics))

	// Se o broker ainda não está pronto ou a conexão cai, tenta de novo
	for ctx.Err() == nil {
		if err := serve(ctx, url, topics); err != nil {
			log.Printf("Session ended: %v; retrying in %s", err, retryInterval)
		}

		// !explain
		select {
		case <-ctx.Done():
		case <-time.After(retryInterval):
		}
	}

	log.Println("Service ended")
}

// Cria as exchanges e filas que os módulos esperam encontrar
func start(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(ingestExchange, "direct", true, false, false, false, nil); err != nil {
		return err
	}

	if err := ch.ExchangeDeclare(topicsExchange, "direct", true, false, false, false, nil); err != nil {
		return err
	}

	if _, err := ch.QueueDeclare(rawQueue, true, false, false, false, nil); err != nil {
		return err
	}

	if err := ch.QueueBind(rawQueue, rawRoutingKey, ingestExchange, false, nil); err != nil {
		return err
	}
	_, err := ch.QueueDeclare(controlQueue, true, false, false, false, nil)

	return err
}

// Abre uma conexão e processa assinaturas
func serve(ctx context.Context, url string, topics map[string]bool) error {
	conn, err := amqp.Dial(url)

	if err != nil {
		return err
	}

	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	if err := start(ch); err != nil {
		return err
	}

	log.Println("Ready and waiting on", controlQueue)

	if err := ch.Qos(prefetch, 0, false); err != nil {
		return err
	}

	msgs, err := ch.Consume(controlQueue, "", false, false, false, false, nil)

	if err != nil {
		return err
	}

	closed := conn.NotifyClose(make(chan *amqp.Error, 1))

	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-closed:
			if e == nil {
				return errors.New("Connection clossed")
			}

			return e
		case d, ok := <-msgs:
			if !ok {
				return errors.New("Consumer closed")
			}

			if err := handleDelivery(ch, d, topics); err != nil {
				return err
			}
		}
	}
}

// Carrega todos os tópicos cadastrados
func loadTopics() map[string]bool {
	topicsStr := strings.TrimSpace(os.Getenv("TOPICS"))

	if topicsStr == "" {
		return nil
	}

	topics := map[string]bool{}

	for _, t := range strings.Split(topicsStr, ",") {
		if t = strings.ToLower(strings.TrimSpace(t)); t != "" {
			topics[t] = true
		}
	}

	return topics
}

// Carrega o ambiente (se definido)
func loadEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}

// Helper para imprimir o nome dos tópicos
func printTopics(topics map[string]bool) string {
	if topics == nil {
		return "todos"
	}

	names := make([]string, 0, len(topics))

	for t := range topics {
		names = append(names, t)
	}

	return strings.Join(names, ",")
}
