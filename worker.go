package main

import (
	"log"
	"scraping-job-handler/internal/jobhandler"

	"flag"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	redisFlag := flag.String("r", "", "Redis Host")
	rabbitFlag := flag.String("q", "", "RabbitMQ Host")

	flag.Parse()

	if *redisFlag == "" || *rabbitFlag == "" {
		panic("Redis and RabbitMQ hosts must be specified.")
	}

	jh := jobhandler.NewJobHandler(jobhandler.JobHandlerConfig{
		RedisAddr:  *redisFlag,
		RabbitAddr: *rabbitFlag,
	})

	if e := jh.Start(60 * 30); e != nil {
		panic(e)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Printf("Shutting down job handler.")
		jh.Stop()
	}()

}
