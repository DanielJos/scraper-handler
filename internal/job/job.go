package job

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

type Job struct {
	JobID        string `json:"jobID"`
	JobType      string `json:"jobType"`
	JobState     string `json:"jobState"`
	UnixCreated  int64  `json:"unixCreated"`
	UnixFinished int64  `json:"unixFinished"`
}

func (j *Job) updateRedis(redis *redis.Client) error {
	if err := redis.Set(context.Background(), j.JobID, j, 0).Err(); err != nil {
		return err
	}

	return nil
}

func (j *Job) Queue(rabbit *amqp.Channel, redis *redis.Client) error {
	body := []byte{}
	if err := json.Unmarshal(body, j); err != nil {
		return err
	}

	// Publish job to rabbitmq
	if err := rabbit.PublishWithContext(
		context.Background(),
		"",
		j.JobType,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Headers: amqp.Table{
				"jobID": j.JobID,
			},
			Body: body,
	}); err != nil {
		return err
	}

	j.JobType = "QUEUED"

	if e := j.updateRedis(redis); e != nil {
		return e
	}

	return nil
}

func (j *Job) Fail(redis *redis.Client) error {
	j.JobState = "FAILED"

	if e := j.updateRedis(redis); e != nil {
		return e
	}

	return nil
}
func (j *Job) Complete(redis *redis.Client) error {
	j.JobState = "COMPLETE"

	if e := j.updateRedis(redis); e != nil {
		return e
	}

	return nil
}
