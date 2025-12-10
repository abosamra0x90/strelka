package main

import (
    "context"
    "encoding/base64"
    "encoding/json"
    "log"

    kafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type RawKafkaMessage struct {
    UUID      string `json:"uuid"`
    Filename  string `json:"filename"`
    DataB64   string `json:"data_base64"`
    Meta      map[string]string `json:"meta"`
}

func startKafkaIngest(coord coord) {
    consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers": "kafka:29092",
        "group.id":          "strelka-ingest",
        "auto.offset.reset": "earliest",
    })
    if err != nil {
        log.Fatalf("Kafka init error: %v", err)
    }

    consumer.Subscribe("raw", nil)

    log.Println("Kafka ingest started... waiting for raw files")

    for {
        msg, err := consumer.ReadMessage(-1)
        if err != nil {
            log.Printf("Kafka read error: %v", err)
            continue
        }

        var raw RawKafkaMessage
        if err := json.Unmarshal(msg.Value, &raw); err != nil {
            log.Printf("Invalid Kafka msg: %v", err)
            continue
        }

        // decode base64
        data, err := base64.StdEncoding.DecodeString(raw.DataB64)
        if err != nil {
            log.Printf("Bad base64: %v", err)
            continue
        }

        // convert to the format Strelka backend expects:
        task := map[string]interface{}{
            "uuid": raw.UUID,
            "filename": raw.Filename,
            "data": data,
            "meta": raw.Meta,
        }

        jsonTask, _ := json.Marshal(task)

        // push to Redis coordinator list “tasks”
        _, err = coord.cli.LPush(context.Background(), "tasks", string(jsonTask)).Result()
        if err != nil {
            log.Printf("Failed to push task to Redis: %v", err)
        } else {
            log.Printf("Pushed %s to Redis tasks FIFO", raw.UUID)
        }
    }
}
