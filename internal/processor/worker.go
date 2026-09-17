package processor

import (
	"fmt"
	"processqueue/internal/task"
	"time"
)

func Process(task_data task.Task) error {

	if task_data.Payload == nil {
		return fmt.Errorf("no data to process")

	}
	switch task_data.Type {
	case "send email":
		time.Sleep(time.Second * 2)
		fmt.Println("Sending email to ", task_data.Payload["to"], " with subject ", task_data.Payload["subject"])
		return nil
	case "process video":
		time.Sleep(time.Second * 2)
		fmt.Println("process the video")
		return nil
	case "generate_pdf":
		fmt.Println("Generating pdf...")
		return nil

	case "":
		return fmt.Errorf("task type is empty")
	default:
		return fmt.Errorf("task is not supported")

	}

}
