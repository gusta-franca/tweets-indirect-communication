.PHONY: all up down clean logs ps build-go build-java build-csharp run-classifier run-collector run-subscriber run-mservice

# Carrega as variáveis do .env (se existir); formato simples CHAVE=valor
-include .env
export TOPICS

# Docker (RabbitMQ + serviço de mensagens)
COMPOSE ?= sudo docker compose

# Go
SERVICE_DIR := message-service
BIN_DIR     := bin
BIN         := $(BIN_DIR)/servico-mensagens
AMQP_URL    ?= amqp://tweets:tweets@localhost:5672/
LOG_FILE    ?= $(CURDIR)/logs/message-service.log
LOG_FILE    := $(abspath $(LOG_FILE))

# Java
JAVA_DIR := classifier
OUT_DIR  := out

# C# (coletor e assinante)
COLLECTOR_DIR  := collector
SUBSCRIBER_DIR := subscriber

all: build-go build-java build-csharp up
	mvn -q compile exec:java

# Sobe o RabbitMQ e o serviço de mensagens (compila a imagem do serviço se preciso)
up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

# Para tudo e apaga os dados do broker (filas, mensagens, assinaturas)
clean:
	$(COMPOSE) down -v

# Imprime o log do serviço de mensagens (logs/message-service.log)
logs:
	mkdir -p $(dir $(LOG_FILE))
	touch $(LOG_FILE)
	tail -f $(LOG_FILE)

ps:
	$(COMPOSE) ps

# Go
build-go:
	mkdir -p $(BIN_DIR)
	cd $(SERVICE_DIR) && CGO_ENABLED=0 go build -o ../$(BIN) ./service

# Java 
build-java:
	mvn -q compile

run-classifier:
	mvn -q compile exec:java

# C#
build-csharp:
	dotnet build $(COLLECTOR_DIR)
	dotnet build $(SUBSCRIBER_DIR)

# Publica os tweets do arquivo (padrão: collector/tweets.json)
# Uso: make run-collector [FILE=outro.json]
run-collector:
	cd $(COLLECTOR_DIR) && AMQP_URL=$(AMQP_URL) dotnet run -- $(FILE)

# Assina tópicos e exibe os tweets recebidos (sem NAME/SUB_TOPICS o programa pergunta)
# Uso: make run-subscriber NAME=alice SUB_TOPICS="futebol voleibol"
run-subscriber:
	cd $(SUBSCRIBER_DIR) && AMQP_URL=$(AMQP_URL) dotnet run -- $(NAME) $(SUB_TOPICS)

# Roda o serviço de mensagens localmente, RabbitMQ fica no docker
run-mservice:
	$(COMPOSE) up -d rabbitmq
	cd $(SERVICE_DIR) && AMQP_URL=$(AMQP_URL) LOG_FILE=$(LOG_FILE) go run ./service
