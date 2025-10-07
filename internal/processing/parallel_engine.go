package processing

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Task represents a unit of work to be processed
type Task interface {
	Execute() error
	ID() string
	Priority() int
}

// TaskResult represents the result of a task execution
type TaskResult struct {
	TaskID string
	Error  error
	Data   interface{}
}

// Worker represents a parallel processing worker
type Worker struct {
	id         int
	queue      chan Task
	resultChan chan TaskResult
	quit       chan bool
	processor  TaskProcessor
	stats      *WorkerStats
}

// TaskProcessor processes tasks
type TaskProcessor interface {
	Process(task Task) (interface{}, error)
}

// WorkerStats contains statistics for a worker
type WorkerStats struct {
	TasksProcessed int64
	TasksFailed    int64
	TotalTime      time.Duration
	ActiveTime     time.Duration
	mu             sync.Mutex
}

// WorkQueue represents a queue of tasks
type WorkQueue struct {
	tasks    []Task
	priority []int
	mu       sync.Mutex
	notEmpty *sync.Cond
}

// NewWorkQueue creates a new work queue
func NewWorkQueue() *WorkQueue {
	q := &WorkQueue{
		tasks: make([]Task, 0),
	}
	q.notEmpty = sync.NewCond(&q.mu)
	return q
}

// Push adds a task to the queue
func (q *WorkQueue) Push(task Task) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Insert task in priority order
	priority := task.Priority()
	inserted := false
	for i, existingPriority := range q.priority {
		if priority > existingPriority {
			q.tasks = append(q.tasks[:i], append([]Task{task}, q.tasks[i:]...)...)
			q.priority = append(q.priority[:i], append([]int{priority}, q.priority[i:]...)...)
			inserted = true
			break
		}
	}

	if !inserted {
		q.tasks = append(q.tasks, task)
		q.priority = append(q.priority, priority)
	}

	q.notEmpty.Signal()
}

// Pop removes and returns the highest priority task
func (q *WorkQueue) Pop() Task {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.tasks) == 0 {
		q.notEmpty.Wait()
	}

	task := q.tasks[0]
	q.tasks = q.tasks[1:]
	q.priority = q.priority[1:]

	return task
}

// TryPop attempts to remove a task without blocking
func (q *WorkQueue) TryPop() (Task, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.tasks) == 0 {
		return nil, false
	}

	task := q.tasks[0]
	q.tasks = q.tasks[1:]
	q.priority = q.priority[1:]

	return task, true
}

// Size returns the number of tasks in the queue
func (q *WorkQueue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

// StealQueue represents a queue that can be stolen from
type StealQueue struct {
	*WorkQueue
}

// Steal attempts to steal a task from another worker's queue
func (q *StealQueue) Steal() (Task, bool) {
	return q.TryPop()
}

// WorkStealingScheduler implements a work-stealing scheduler
type WorkStealingScheduler struct {
	workers     []*Worker
	queues      []*StealQueue
	globalQueue *WorkQueue
	resultChan  chan TaskResult
	quit        chan bool
	wg          sync.WaitGroup
	processor   TaskProcessor
	stats       *SchedulerStats
	config      SchedulerConfig
}

// SchedulerConfig contains configuration for the scheduler
type SchedulerConfig struct {
	NumWorkers       int           // Number of worker goroutines
	QueueSize        int           // Size of per-worker queues
	StealInterval    time.Duration // Interval between steal attempts
	MaxStealAttempts int           // Maximum steal attempts per interval
	LoadBalance      bool          // Enable load balancing
}

// DefaultSchedulerConfig returns a default configuration
func DefaultSchedulerConfig() SchedulerConfig {
	return SchedulerConfig{
		NumWorkers:       runtime.NumCPU(),
		QueueSize:        1000,
		StealInterval:    10 * time.Millisecond,
		MaxStealAttempts: 3,
		LoadBalance:      true,
	}
}

// SchedulerStats contains statistics for the scheduler
type SchedulerStats struct {
	TasksSubmitted int64
	TasksCompleted int64
	TasksFailed    int64
	StealAttempts  int64
	StealSuccesses int64
	TotalRuntime   time.Duration
	ActiveWorkers  int32
	StartTime      time.Time
	mu             sync.Mutex
}

// NewWorkStealingScheduler creates a new work-stealing scheduler
func NewWorkStealingScheduler(processor TaskProcessor, config SchedulerConfig) *WorkStealingScheduler {
	if config.NumWorkers <= 0 {
		config.NumWorkers = runtime.NumCPU()
	}
	if config.QueueSize <= 0 {
		config.QueueSize = 1000
	}
	if config.StealInterval <= 0 {
		config.StealInterval = 10 * time.Millisecond
	}
	if config.MaxStealAttempts <= 0 {
		config.MaxStealAttempts = 3
	}

	scheduler := &WorkStealingScheduler{
		processor:   processor,
		resultChan:  make(chan TaskResult, config.NumWorkers*10),
		quit:        make(chan bool),
		config:      config,
		stats:       &SchedulerStats{StartTime: time.Now()},
		workers:     make([]*Worker, config.NumWorkers),
		queues:      make([]*StealQueue, config.NumWorkers),
		globalQueue: NewWorkQueue(),
	}

	// Create workers and their queues
	for i := 0; i < config.NumWorkers; i++ {
		queue := &StealQueue{NewWorkQueue()}
		scheduler.queues[i] = queue

		worker := &Worker{
			id:         i,
			queue:      make(chan Task, config.QueueSize),
			resultChan: scheduler.resultChan,
			quit:       make(chan bool),
			processor:  processor,
			stats:      &WorkerStats{},
		}
		scheduler.workers[i] = worker
	}

	return scheduler
}

// Start starts the scheduler and all workers
func (s *WorkStealingScheduler) Start(ctx context.Context) {
	// Start workers
	for i, worker := range s.workers {
		s.wg.Add(1)
		go s.runWorker(ctx, worker, s.queues[i])
	}

	// Start load balancer if enabled
	if s.config.LoadBalance {
		s.wg.Add(1)
		go s.runLoadBalancer(ctx)
	}
}

// Stop stops the scheduler and all workers
func (s *WorkStealingScheduler) Stop() {
	close(s.quit)
	s.wg.Wait()
	close(s.resultChan)
}

// Submit submits a task to the scheduler
func (s *WorkStealingScheduler) Submit(task Task) error {
	atomic.AddInt64(&s.stats.TasksSubmitted, 1)

	// Try to submit to a worker queue directly
	if s.config.LoadBalance {
		// Find the least loaded worker
		minQueueSize := int(^uint(0) >> 1) // Max int
		targetWorker := 0

		for i, queue := range s.queues {
			queueSize := queue.Size()
			if queueSize < minQueueSize {
				minQueueSize = queueSize
				targetWorker = i
			}
		}

		// Submit to the least loaded worker if queue is not full
		if minQueueSize < s.config.QueueSize {
			s.queues[targetWorker].Push(task)
			return nil
		}
	}

	// Submit to global queue as fallback
	s.globalQueue.Push(task)
	return nil
}

// Results returns the result channel
func (s *WorkStealingScheduler) Results() <-chan TaskResult {
	return s.resultChan
}

// Stats returns scheduler statistics
func (s *WorkStealingScheduler) Stats() SchedulerStats {
	s.stats.mu.Lock()
	defer s.stats.mu.Unlock()

	return SchedulerStats{
		TasksSubmitted: s.stats.TasksSubmitted,
		TasksCompleted: s.stats.TasksCompleted,
		TasksFailed:    s.stats.TasksFailed,
		TotalRuntime:   time.Since(s.stats.StartTime),
		ActiveWorkers:  atomic.LoadInt32(&s.stats.ActiveWorkers),
	}
}

// runWorker runs a worker goroutine
func (s *WorkStealingScheduler) runWorker(ctx context.Context, worker *Worker, queue *StealQueue) {
	defer s.wg.Done()
	atomic.AddInt32(&s.stats.ActiveWorkers, 1)
	defer atomic.AddInt32(&s.stats.ActiveWorkers, -1)

	ticker := time.NewTicker(s.config.StealInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.quit:
			return
		default:
			// Try to get a task from local queue
			task, ok := queue.TryPop()
			if !ok {
				// Try to steal from other workers
				task = s.stealTask(worker.id)
				if task == nil {
					// Try global queue
					task, ok = s.globalQueue.TryPop()
					if !ok {
						// No tasks available, wait
						select {
						case <-ctx.Done():
							return
						case <-s.quit:
							return
						case <-ticker.C:
							continue
						}
					}
				}
			}

			if task != nil {
				s.executeTask(worker, task)
			}
		}
	}
}

// stealTask attempts to steal a task from another worker
func (s *WorkStealingScheduler) stealTask(workerID int) Task {
	for i := 0; i < s.config.MaxStealAttempts; i++ {
		// Try a random worker
		targetID := (workerID + i + 1) % len(s.queues)
		if targetID == workerID {
			continue
		}

		atomic.AddInt64(&s.stats.StealAttempts, 1)
		if task, ok := s.queues[targetID].Steal(); ok {
			atomic.AddInt64(&s.stats.StealSuccesses, 1)
			return task
		}
	}
	return nil
}

// executeTask executes a task and sends the result
func (s *WorkStealingScheduler) executeTask(worker *Worker, task Task) {
	start := time.Now()

	result, err := s.processor.Process(task)

	duration := time.Since(start)

	// Update worker stats
	worker.stats.mu.Lock()
	worker.stats.TasksProcessed++
	worker.stats.TotalTime += duration
	worker.stats.ActiveTime += duration
	if err != nil {
		worker.stats.TasksFailed++
	}
	worker.stats.mu.Unlock()

	// Update scheduler stats
	if err != nil {
		atomic.AddInt64(&s.stats.TasksFailed, 1)
	} else {
		atomic.AddInt64(&s.stats.TasksCompleted, 1)
	}

	// Send result
	select {
	case s.resultChan <- TaskResult{
		TaskID: task.ID(),
		Error:  err,
		Data:   result,
	}:
	case <-s.quit:
		return
	}
}

// runLoadBalancer runs the load balancer
func (s *WorkStealingScheduler) runLoadBalancer(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.config.StealInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.quit:
			return
		case <-ticker.C:
			s.balanceLoad()
		}
	}
}

// balanceLoad balances tasks between workers
func (s *WorkStealingScheduler) balanceLoad() {
	// Find workers with most and least tasks
	maxTasks := 0
	minTasks := int(^uint(0) >> 1) // Max int
	maxWorker := 0
	minWorker := 0

	for i, queue := range s.queues {
		size := queue.Size()
		if size > maxTasks {
			maxTasks = size
			maxWorker = i
		}
		if size < minTasks {
			minTasks = size
			minWorker = i
		}
	}

	// If imbalance is significant, move tasks
	if maxTasks-minTasks > 2 && maxTasks > 0 {
		// Steal from max worker and give to min worker
		if task, ok := s.queues[maxWorker].Steal(); ok {
			s.queues[minWorker].Push(task)
		}
	}
}

// PipelineProcessor implements a pipeline processing pattern
type PipelineProcessor struct {
	stages []PipelineStage
}

// PipelineStage represents a stage in the processing pipeline
type PipelineStage interface {
	Process(data interface{}) (interface{}, error)
	Name() string
}

// NewPipelineProcessor creates a new pipeline processor
func NewPipelineProcessor(stages ...PipelineStage) *PipelineProcessor {
	return &PipelineProcessor{
		stages: stages,
	}
}

// Process processes data through all pipeline stages
func (p *PipelineProcessor) Process(task Task) (interface{}, error) {
	data := interface{}(task)

	for _, stage := range p.stages {
		result, err := stage.Process(data)
		if err != nil {
			return nil, fmt.Errorf("pipeline stage %s failed: %w", stage.Name(), err)
		}
		data = result
	}

	return data, nil
}

// BatchProcessor processes tasks in batches
type BatchProcessor struct {
	processor TaskProcessor
	batchSize int
	timeout   time.Duration
}

// NewBatchProcessor creates a new batch processor
func NewBatchProcessor(processor TaskProcessor, batchSize int, timeout time.Duration) *BatchProcessor {
	return &BatchProcessor{
		processor: processor,
		batchSize: batchSize,
		timeout:   timeout,
	}
}

// Process processes a task (for compatibility with TaskProcessor interface)
func (p *BatchProcessor) Process(task Task) (interface{}, error) {
	return p.processor.Process(task)
}

// ProcessBatch processes a batch of tasks
func (p *BatchProcessor) ProcessBatch(tasks []Task) ([]TaskResult, error) {
	results := make([]TaskResult, len(tasks))

	var wg sync.WaitGroup
	for i, task := range tasks {
		wg.Add(1)
		go func(index int, t Task) {
			defer wg.Done()

			result, err := p.processor.Process(t)
			results[index] = TaskResult{
				TaskID: t.ID(),
				Error:  err,
				Data:   result,
			}
		}(i, task)
	}

	wg.Wait()
	return results, nil
}
