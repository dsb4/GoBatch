// Unit tests for pipe.go
// Run using the command  'go test'
package pipe

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

// TextProcessor. BatchProcessor implementation.
// Processes a batch of jobs containing text data. For each job it returns the contained string, prepending
// "result: "
// Returns nil error
type TextProcessor struct {
	// slice containing number of jobs that were present in each batch
	JobsPerBatch []uint
	// Sets an artificial processing time for a batch
	ProcessingTime time.Duration
	// Sets an error value to return
	ErrorText string
}

// Error returned by TextProcessor
type TextProcessorError struct {
	err string
}

// Implements Error
func (t TextProcessorError) Error() string {
	return t.err
}

func (p *TextProcessor) Process(jobs []Job) ([]BatchResult, error) {
	var err error = nil
	// record number of jobs in batch
	p.JobsPerBatch = append(p.JobsPerBatch, uint(len(jobs)))

	// if processing delay is set, delay
	if p.ProcessingTime > 0 {
		time.Sleep(p.ProcessingTime)
	}

	// if error text is set, set the error
	if p.ErrorText != "" {
		err = TextProcessorError{p.ErrorText}
	}

	// create result be prepending 'result: ' to the data contained in each job
	results := make([]BatchResult, len(jobs), cap(jobs))
	for i, v := range jobs {
		results[i].Id = v.Id
		var str interface{} = fmt.Sprintf("result: %v", *v.Data)
		results[i].Result = &str
	}

	return results, err
}

// Test NewPipe with invalid arguments
// Expect error to be returned.
func TestNewPipeArgErrors(t *testing.T) {
	// invalid maxDuration.
	// Expiect pipe=nil, errCh=nil, err=PipeCreateError
	pipe, errCh, err := NewPipe(-1, 1, &TextProcessor{})
	if pipe != nil {
		t.Errorf("Got %T, wanted nil", pipe)
	}
	if errCh != nil {
		t.Errorf("Got %T, wanted nil", errCh)
	}
	if reflect.TypeOf(err) != reflect.TypeOf(PipeCreateError{}) {
		t.Errorf("Got %T, wanted %T", err, PipeCreateError{})
	}

	// invalid maxJobs.
	// Expiect pipe=nil, errCh=nil, err=PipeCreateError
	pipe, errCh, err = NewPipe(1, 0, &TextProcessor{})
	if pipe != nil {
		t.Errorf("Got %T, wanted nil", pipe)
	}
	if errCh != nil {
		t.Errorf("Got %T, wanted nil", errCh)
	}
	if reflect.TypeOf(err) != reflect.TypeOf(PipeCreateError{}) {
		t.Errorf("Got %T, wanted %T", err, PipeCreateError{})
	}
}

// Test NewPipe with valid arguments
// Expect pipe to be created. No errors.
func TestNewPipeSuccess(t *testing.T) {
	// create pipe with valid args
	// Expect pipe!=nil, errCh!=nil, err=nil
	pipe, errCh, err := NewPipe(1, 1, &TextProcessor{})
	if pipe == nil {
		t.Error("Got nil, wanted Pipe.")
	}
	if errCh == nil {
		t.Error("Got nil, wanted 'chan error'.")
	}
	if err != nil {
		t.Errorf("Got %T, wanted nil.", err)
	}
	pipe.Shutdown()
}

// Test that jobs are correctly split into batches
// Set maxJobs=3. Push 6 jobs into the pipe.
// Expect 2 batches to processed, each batch containing 3 jobs.
func TestBatching(t *testing.T) {
	// create pipe with maxJobs=3
	textProcessor := &TextProcessor{}
	pipe, _, _ := NewPipe(2*time.Second, 3, textProcessor)
	// push 6 jobs into the pipe. This should result in 2 batches of 3 jobs being processed.
	var pl1 interface{} = "Data1"
	j1 := NewJob(&pl1)
	pipe.Push(j1)
	var pl2 interface{} = "Data2"
	j2 := NewJob(&pl2)
	pipe.Push(j2)
	var pl3 interface{} = "Data3"
	j3 := NewJob(&pl3)
	pipe.Push(j3)
	var pl4 interface{} = "Data4"
	j4 := NewJob(&pl4)
	pipe.Push(j4)
	var pl5 interface{} = "Data5"
	j5 := NewJob(&pl5)
	pipe.Push(j5)
	var pl6 interface{} = "Data6"
	j6 := NewJob(&pl6)
	res6 := pipe.Push(j6)

	// wait for job6 to complete (not instant)
	<-res6.Complete

	// Expect 2 batches, each batch containing 3 jobs
	if len(textProcessor.JobsPerBatch) != 2 {
		t.Errorf("Got %v, wanted 2 batches to be processed", len(textProcessor.JobsPerBatch))
		if textProcessor.JobsPerBatch[0] != 3 {
			t.Errorf("Got %v, wanted 3 jobs to be in first batch", textProcessor.JobsPerBatch[0])
		}
		if textProcessor.JobsPerBatch[1] != 3 {
			t.Errorf("Got %v, wanted 3 jobs to be in second batch", textProcessor.JobsPerBatch[1])
		}
	}

	pipe.Shutdown()
}

// Test that the first batch is processed before the second batch.
// Create a new pipe. Set maxDuration to 2 seconds. Push 5 jobs.
// Once job3 is complete, wait one second and then check that job 4 is not complete.
// We do not expect job 4 to complete until about 2 seconds after job 3, since
// job 4 is part of the second batch (incomplete size-wise) which will be processed
// only after maxDuration is reached.
func TestMaxDurationBatching(t *testing.T) {
	// Create pipe. Push 5 jobs into the pipe
	pipe, _, _ := NewPipe(2*time.Second, 3, &TextProcessor{})
	var pl1 interface{} = "Data1"
	j1 := NewJob(&pl1)
	pipe.Push(j1)
	var pl2 interface{} = "Data2"
	j2 := NewJob(&pl2)
	pipe.Push(j2)
	var pl3 interface{} = "Data3"
	j3 := NewJob(&pl3)
	res3 := pipe.Push(j3)
	var pl4 interface{} = "Data4"
	j4 := NewJob(&pl4)
	res4 := pipe.Push(j4)
	var pl5 interface{} = "Data5"
	j5 := NewJob(&pl5)
	pipe.Push(j5)

	// block until job 3 is complete
	if <-res3.Complete {
		// Expect job 3 to have result "result: Data3"
		if fmt.Sprint(*res3.Result) != "result: Data3" {
			t.Errorf("Got '%s', wanted 'result: Data3'", *res3.Result)
		}
	}

	// sleep for 1 second
	time.Sleep(1 * time.Second)
	// check that job 4 does not have its result set yet.
	// Expect res4.Result = nil
	if res4.Result != nil {
		t.Errorf("Got %s, wanted nil", *res4.Result)
	}

	// sleep for 2 more seconds
	time.Sleep(2 * time.Second)
	// Since maxDuration has been passed, we expect res4 to have a result, indicating batch 2
	// has been processed.
	if fmt.Sprint(*res4.Result) != "result: Data4" {
		t.Errorf("Got %s, wanted 'result: Data4'", *res4.Result)
	}

	pipe.Shutdown()
}

// Test shutdown behaviour.
// Expect shutdown to block until all jobs in the pipe are processed.
func TestShutdown(t *testing.T) {
	// create a text processor that takes 2 seconds to process a batch
	textProcessor := &TextProcessor{}
	textProcessor.ProcessingTime = 2 * time.Second
	// Create pipe with maxJobs=1. Push 2 jobs into the pipe. Should have 2 batches running.
	pipe, _, _ := NewPipe(2*time.Second, 1, textProcessor)
	var pl1 interface{} = "Data1"
	j1 := NewJob(&pl1)
	pipe.Push(j1)
	var pl2 interface{} = "Data2"
	j2 := NewJob(&pl2)
	res2 := pipe.Push(j2)

	// check that job2 does not have a result yet
	// Expect res2.Result = nil
	if res2.Result != nil {
		t.Errorf("Got %s, wanted nil", *res2.Result)
	}

	// Shutdown the pipe. This should block until all jobs in the pipe have been completed
	// approx 2 secs
	pipe.Shutdown()

	// Since shudtown is complete, we expect res4 to have a result, the jobs in the pipe
	// were successfully processed.
	if fmt.Sprint(*res2.Result) != "result: Data2" {
		t.Errorf("Got %s, wanted 'result: Data2'", *res2.Result)
	}
}

// Test that errors generated in the BatchProcessor are passed back to the pipe user via
// the error channel.
func TestErrorReporting(t *testing.T) {
	// create a text processor that returns an error while processing a batch
	textProcessor := &TextProcessor{}
	textProcessor.ErrorText = "Test Error"
	// Create pipe with maxJobs=1.
	pipe, errorCh, _ := NewPipe(2*time.Second, 1, textProcessor)

	// drain errorCh
	go func() {
		for run := true; run; {
			err := <-errorCh
			// expect error 'Test Error' to be emitted when batch is processed.
			if err.Error() != "Test Error" {
				t.Errorf("Got %s, wanted 'Test Error'", err.Error())
			}
			pipe.Shutdown()
			run = false
		}
	}()

	// Push 1 jobs into the pipe. Should have 1 batches running.
	// This should generate an error
	var pl1 interface{} = "Data1"
	j1 := NewJob(&pl1)
	pipe.Push(j1)
}
