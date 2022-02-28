# GoBatch
Micro-batching library in Go

# Description
This package is a batch processing library. Instead of processing jobs one-by-one, this library groups them into
configurable size batches. This is convenient to optimise I/O operations with databases and internet services.

# How to Use
This package exposes the *Pipe* struct which exposes one method: *Push(job Job)* which returns a pointer to a
*JobResult*. *JobResult* is a struct which will contain the result of the job once the job has been processed.

## Creating a Job:
Call the function *NewJob* to create a job. The function takes a pointer to the *Data* which is of type
interface{}. The *Data* is the data which is to be processed.

## Using a JobResult:
A *JobResult* pointer is returned when a job is pushed into the pipe using the *Push(job Job)* method. To
access a *JobResult* read from the *Complete* channel. When that channel recieves *true*, then the job has been
completed and the *Result* member will point to the result.

## Creating a BatchProcessor:
In order to configure the pipe a BatchProcessor is required. This is user implemented and the interface is
specified in this package. The BatchProcessor must accept an array of *Jobs* and return an array of
*BatchResults* and an error. If no error is encountered, the error must be nil. *BatchResult* is a struct
containing a pointer to the result. It also contains a member *Id* which must be set to the Id of the Job
to which the *BatchResult* corresponds.

## Configuring the pipe:
To create a pipe, call the function: *NewPipe*. This function initialises the pipe and configures it using
the arguments that it is passed. The following arguments are required:
- maxDuration: This is the maximum time the pipe should wait before processing a batch
- maxJobs: This is the maximum number of jobs the pipe should accept before processing a batch
- batchProcessor: This is the "BatchProcessor" that the pipe will use to process batches

*NewPipe* returns the created pipe, and an error channel to which any errors will be sent.

## Shutting down a pipe
To shutdown a pipe call the *Shutdown()* method. This will allow all submitted jobs to submit, terminate the pipe
and then return.

# Building and Testing
## Building the library
Navigate to the pipe folder
Run the command `go build`
## Running the test suite
Navigate to the pipe folder
Run the command `go test`