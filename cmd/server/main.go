package main

import (
	"fmt"
	"log"
	"os"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connStr := "amqp://guest:guest@localhost:5672/"
	qpConn, err := amqp.Dial(connStr)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer qpConn.Close()

	fmt.Println("Connection was successful")

	chn, err := qpConn.Channel()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = pubsub.SubscribeGob(
		qpConn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		fmt.Sprintf("%s.*", routing.GameLogSlug),
		pubsub.Durable,
		handlerGameLog(),
	)
	if err != nil {
		log.Fatalf("Error occurred: %v\n", err)
	}

	gamelogic.PrintServerHelp()

	for {
		input := gamelogic.GetInput()
		if len(input) > 0 {
			if input[0] == "pause" {
				fmt.Println("Sending pause message")
				pubsub.PublishJSON(chn, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
					IsPaused: true,
				})
			} else if input[0] == "resume" {
				fmt.Println("Sending resume message")
				pubsub.PublishJSON(chn, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
					IsPaused: false,
				})
			} else if input[0] == "quit" {
				fmt.Println("Exiting")
				break
			} else {
				fmt.Println("I don't understand that command")
			}
		}
	}

	fmt.Println("Progam is shutting down")
}

func handlerGameLog() func(routing.GameLog) pubsub.AckType {
	return func(gl routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")
		err := gamelogic.WriteLog(gl)
		if err != nil {
			fmt.Println(err)
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}
