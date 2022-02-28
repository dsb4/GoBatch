// Package: pipe
// Author: dsb4
// This package is a batch processing library. Instead of processing jobs one-by-one, this library groups them into
// configurable size batches. This is convenient to optimise I/O operations with databases and internet services.

// This package exposes the "Pipe" struct which exposes one method: "Push(job Job)" which returns a pointer to a
// "JobResult". "JobResult" is a struct which will contain the result of the job once the job has been processed.

// Creating a Job:
// Call the function "NewJob" to create a job. The function takes a pointer to the "Data" which is of type
// interface{}. The "Data" is the data which is to be processed.

// Using a JobResult:
// A "JobResult" pointer is returned when a job is pushed into the pipe using the "Push(job Job)" method. To
// access a JobResult read from the "Complete" channel. When that channel recieves "true", then the job has been
// completed and the "Result" member will point to the result.

// Creating a BatchProcessor:
// In order to configure the pipe a BatchProcessor is required. This is user implemented and the interface is
// specified in this package. The BatchProcessor must accept an array of "Jobs" and return an array of
// "BatchResults" and an error. If no error is encountered, the error must be nil. "BatchResult" is a struct
// containing a pointer to the result. It also contains a member "Id" which must be set to the Id of the Job
// to which the "BatchResult" corresponds.

// Configuring the pipe:
// To create a pipe, call the function: "NewPipe". This function initialises the pipe and configures it using
// the arguments that it is passed. The following arguments are required:
// 		maxDuration: This is the maximum time the pipe should wait before processing a batch
// 		maxJobs: This is the maximum number of jobs the pipe should accept before processing a batch
// 		batchProcessor: This is the "BatchProcessor" that the pipe will use to process batches
// "NewPipe" returns the created pipe, and an error channel to which any errors will be sent, and an error struct.
// If invalid arguemnts are supplied, the error struct will be populated, if not it will be nil
//
// Shutting down a pipe:
// To shutdown a pipe call the *Shutdown()* method. This will allow all submitted jobs to submit, terminate the pipe
// and then return.
package pipe

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

// BatchProcessor Interface
type BatchProcessor interface {
	Process([]Job) ([]BatchResult, error)
}

// BatchResult Definition
type BatchResult struct {
	// Public Members
	Id     uuid.UUID    // Id of the Job for which this is the result
	Result *interface{} // Pointer to the result.
}

// Job Struct
type Job struct {
	// Public Members
	Id   uuid.UUID    // Id of the Job
	Data *interface{} // Pointer to the data to be processed by the BatchProcessor

	// Private Members
	complete chan bool  // Channel to communicate the completion status once the job has been processed.
	result   *JobResult // Pointer to the corresponding JobResult
}

// Function to create a new Job
// Takes a pointer to the data which is to be processed by the BatchProcessor.
func NewJob(data *interface{}) Job {
	// create a new Job
	j := Job{}
	// Create a uuid to assign to the Job
	j.Id = uuid.New()
	// store pointer to the data which is to be processed.
	j.Data = data
	return j
}

// JobResult struct
type JobResult struct {
	// Public Members
	Id       uuid.UUID    // Id of the corresponding Job
	Result   *interface{} // Pointer to the result
	Complete chan bool    // Channel on which to receive the completion status of the corresponding job
}

// Pipe struct
type Pipe struct {
	// Config Items
	maxDuration    time.Duration  // maximum Duration before processing another batch (even if batch is not full)
	maxJobs        uint           // maximum number of Jobs in a batch. Process batch when job count reaches this number
	batchProcessor BatchProcessor // the BatchProcessor to process a batch of jobs
	// Channels
	errorCh    chan error // error channel for passing errors back to the pipe user
	jobCh      chan Job   // job channel for passing jobs to the processor
	shutdownCh chan bool  // shutdown channel for telling goroutines to terminate
	// Synchronisation
	waitGroup sync.WaitGroup // Waitgroup to wait for all goroutines to finish
}

// Pipe.process method (Private)
// Accepts an array of Jobs and sends them to the BatchProcessor
// Once the jobs have been processed, it converts the results into JobResults, and saves the reference in the
// corresponding job. It then indicates that the job is complete by writing "true" to the completion channel.
// This notifies the pipe user that the JobResult is ready for reading.
func (p *Pipe) process(jobs []Job) {
	defer p.waitGroup.Done()
	// process Jobs using the batchProcessor
	results, err := p.batchProcessor.Process(jobs)
	// if there is an error, write it to the error channel. This will be forwarded to the pipe user.
	if err != nil {
		p.errorCh <- err
	}

	// For each result, find the corresponding job
	for _, res := range results {
		// find job with matching uuid
		for _, job := range jobs {
			if job.Id == res.Id {
				// got matching job!
				// copy result to the JobResult struct
				job.result.Result = res.Result
				// indicate that the job is complete. JobResult is ready to read.
				job.result.Complete <- true
			}
		}
	}
}

// Pipe.run method (Private)
// Batches jobs and sends them to be processed.
func (p *Pipe) run() {
	// Tell waitgroup when routine finishes.
	defer p.waitGroup.Done()
	// create jobs buffer
	jobs := make([]Job, 0, p.maxJobs)
	// initialise timeout
	timeOut := time.After(p.maxDuration)

	// logic to process jobs
	sendJobs := func() {
		jobsCopy := make([]Job, len(jobs), cap(jobs))
		copy(jobsCopy, jobs)
		p.waitGroup.Add(1)
		go p.process(jobsCopy)
		timeOut = time.After(p.maxDuration)
		// reset jobs buffer
		jobs = jobs[:0]
	}

	// keep running the pipe batching logic until told to terminate by shutdown channel
	for run := true; run; {
		select {
		case job := <-p.jobCh:
			jobs = append(jobs, job)
			// if we have reached max jobs for a batch, process the batch
			if len(jobs) >= int(p.maxJobs) {
				sendJobs()
			}
		case <-p.shutdownCh:
			// if we have been asked to shutdown, but there are still jobs in the pipe, process them immediately
			if len(jobs) > 0 {
				sendJobs()
			}
			// terminate loop
			run = false
		case <-timeOut:
			// if we have reached maxDuration since last batch processing, then send jobs if there are any.
			if len(jobs) > 0 {
				sendJobs()
			} else {
				// if we have reached maxDuration since last batch processing but there are no jobs in the pipe,
				// reset the timeout to 500ms and try again.
				timeOut = time.After(500 * time.Millisecond)
			}

		}
	}

}

// Pipe.Push method (Public)
// Push a job into the pipe.
// Returns a JobResult pointer. This is a promise that will contain the result of the Job once the job has been
// processed. Completion is indicated by the "Complete" channel within the JobResult.
func (p *Pipe) Push(job Job) *JobResult {
	// create completion channel (with buffer so we dont block, if the user doesn't read)
	completionCh := make(chan bool, 1)

	// add completion channel to the job
	job.complete = make(chan bool, 1)

	// create a JobResult
	result := JobResult{job.Id, nil, completionCh}
	// save reference to the result in the Job
	job.result = &result
	// send job to the main go routine for processing
	p.jobCh <- job
	return &result
}

// Pipe.Shutdown method (Public)
// Tells the pipe to shutdown. The pipe will process any unprocessed jobs. When all jobs are processed, this method
// will return.
func (p *Pipe) Shutdown() {
	p.shutdownCh <- true
	p.waitGroup.Wait()
}

// Create a new Pipe.
//The following arguments are required:
//		maxDuration: This is the maximum time the pipe should wait before processing a batch
//		maxJobs: This is the maximum number of jobs the pipe should accept before processing a batch
//		batchProcessor: This is the "BatchProcessor" that the pipe will use to process batches
// returns the created pipe, an error channel to which any errors will be sent, and an error struct.
// If invalid arguemnts are supplied, the error struct will be populated, if not it will be nil
func NewPipe(maxDuration time.Duration, maxJobs uint, batchProcessor BatchProcessor) (*Pipe, <-chan error, error) {
	var err error = nil

	// Validate arguments
	switch {
	case maxDuration < 0:
		err = PipeCreateError{"Invalid Parameter. maxDuration must be >= 0."}
	case maxJobs < 1:
		err = PipeCreateError{"Invalid Parameter. maxJobs must be > 1."}
	}

	// if arg validation failed, return error.
	if err != nil {
		return nil, nil, err
	}

	// initialise pipe variables
	p := Pipe{}
	p.maxDuration = maxDuration
	p.maxJobs = maxJobs
	p.batchProcessor = batchProcessor
	p.errorCh = make(chan error)

	p.jobCh = make(chan Job)
	p.shutdownCh = make(chan bool)

	// run main processing goroutine
	p.waitGroup.Add(1)
	go p.run()

	// return reference to the pipe and the error channel
	return &p, p.errorCh, err
}

// Error struct to use if there is an error creating a pipe using the NewPipe function
type PipeCreateError struct {
	err string
}

// PipeCreateError method Error(). Implements error interface.
func (t PipeCreateError) Error() string {
	return t.err
}
