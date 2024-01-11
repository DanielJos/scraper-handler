package jobhandler

import (
	"encoding/json"
	"fmt"
	"time"
	"log"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"

	"scraping-job-handler/internal/job"
)

type JobHandlerConfig struct {
	RedisAddr string
	RabbitAddr string
}

type JobResponse struct {
	ID string `json:"jobID"`
	State string `json:"jobState"`
	Data string `json:"jobData"`
}

type JobHandler struct {
	Redis  *redis.Client
	Rabbit *amqp.Channel

	quit chan struct{}

	jobs map[string]job.Job
}

func NewJobHandler(config JobHandlerConfig) *JobHandler {
	redis := redis.NewClient(&redis.Options{
		Addr:     config.RedisAddr,
		Password: "",
		DB:       0,
	})

	rabbitClient, err := amqp.Dial(config.RabbitAddr)
	if err != nil {
		log.Panicln("error connecting to rabbitmq.", err)
	}

	rabbit, e := rabbitClient.Channel()
	if e != nil {
		log.Panicln("error connecting to rabbitmq.", e)
	}

	return &JobHandler{
		Redis:  redis,
		Rabbit: rabbit,
		jobs:   make(map[string]job.Job),
		quit:   make(chan struct{}),
	}
}

func (jh *JobHandler) Stop() {
	close(jh.quit)
	jh.Rabbit.Close()
	jh.Redis.Close()
}

func (jh *JobHandler) GenerateJob (JobType string) (job.Job, error) {
	j := job.Job{
		JobID:   uuid.New().String(),
		JobType: JobType,
		JobState: "CREATED",
	}

	jh.jobs[j.JobID] = j

	return j, nil
}

func (jh *JobHandler) QueueJob (jobID string) error {
	j, ok := jh.jobs[jobID]
	if !ok {
		return fmt.Errorf("jobID %s not found", jobID)
	}

	if err := j.Queue(jh.Rabbit, jh.Redis); err != nil {
		return err
	}

	jh.jobs[jobID] = j

	return nil
}

// At the interval, create a new job and queue it
func (jh *JobHandler) Start(intervalSeconds int) error {
	if err := jh.subscribeJobResponses(); err != nil {
		return err
	}

	for {
		select {
		case <-jh.quit:
			return nil
		default:
			j, err := jh.GenerateJob("PP")
			if err != nil {
				return err
			}

			if err := jh.QueueJob(j.JobID); err != nil {
				return err
			}

			j2, err := jh.GenerateJob("News")
			if err != nil {
				return err
			}

			if err := jh.QueueJob(j2.JobID); err != nil {
				return err
			}
		}

		time.Sleep(time.Duration(intervalSeconds) * time.Second)
	}
}

func (jh *JobHandler) subscribeJobResponses () error {
	msgs, err := jh.Rabbit.Consume(
		"job-responses",
		"",
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-jh.quit:
				return
			case msg := <-msgs:
				jobID, ok := msg.Headers["jobID"].(string)
				if !ok {
					fmt.Println("jobID not found in headers")
					continue
				}
	
				j, ok := jh.jobs[jobID]
				if !ok {
					fmt.Println("jobID not found in jobs")
					continue
				}

				var jr JobResponse
	
				if err := json.Unmarshal(msg.Body, &jr); err != nil {
					fmt.Println("error unmarshalling job response", err)
					continue
				}
	
				switch jr.State {
				case "SUCCESS":
					j.Complete(jh.Redis)
				case "FAILURE":
					j.Fail(jh.Redis)
				default:
					fmt.Println("unknown job response state", jr.State)
					continue
				}
			}
		}
	}()

	return nil
}