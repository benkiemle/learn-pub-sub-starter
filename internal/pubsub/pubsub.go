package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int
type AckType int

const (
	Durable   SimpleQueueType = 0
	Transient SimpleQueueType = 1

	Ack         AckType = 1
	NackRequeue AckType = 2
	NackDiscard AckType = 3
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}

	return ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        data,
	})
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(val)
	if err != nil {
		return err
	}

	return ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body:        buffer.Bytes(),
	})
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	chn, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	deadLetter := amqp.Table{}
	deadLetter["x-dead-letter-exchange"] = "peril_dlx"

	queue, err := chn.QueueDeclare(queueName, queueType == Durable, queueType == Transient, queueType == Transient, false, deadLetter)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = chn.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return chn, queue, nil
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
	decoder func([]byte) (T, error),
) error {
	chn, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	chn.Qos(10, 0, true)

	deliveryChan, err := chn.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for delivery := range deliveryChan {
			body, err := decoder(delivery.Body)
			if err != nil {
				fmt.Println(err)
				return
			}
			ackType := handler(body)
			switch ackType {
			case Ack:
				fmt.Println("Ack message")
				delivery.Ack(false)
			case NackRequeue:
				fmt.Println("NackRequeue message")
				delivery.Nack(false, true)
			case NackDiscard:
				fmt.Println("NackDiscard message")
				delivery.Nack(false, false)
			}
		}
	}()

	return nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, retrieveFromJSON)
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, retrieveFromGob)
}

func retrieveFromJSON[T any](data []byte) (T, error) {
	var model T
	err := json.Unmarshal(data, &model)
	return model, err
}

func retrieveFromGob[T any](data []byte) (T, error) {
	buffer := bytes.NewBuffer(data)
	var model T
	decoder := gob.NewDecoder(buffer)
	err := decoder.Decode(&model)
	return model, err
}
