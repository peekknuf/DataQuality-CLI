package processing

import (
	"context"
	"fmt"
	"math/rand"
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
	id               int
	resultChan       chan TaskResult
	quit             chan bool
	processor        TaskProcessor
	stats            *WorkerStats
	idleIntervals    int           // Count of consecutive idle intervals
	backoffDuration  time.Duration // Current backoff duration
	iterationCounter int           // Counter for batch processing and timing
	tickerSignal     chan struct{} // Signal from shared ticker
}

// TaskProcessor processes tasks
type TaskProcessor interface {
	Process(task Task) (interface{}, error)
}

// WorkerStats contains statistics for a worker
type WorkerStats struct {
	TasksProcessed int64         // atomic
	TasksFailed    int64         // atomic
	TotalTime      int64         // atomic (nanoseconds)
	ActiveTime     int64         // atomic (nanoseconds)
	mu             sync.Mutex    // Only for complex aggregations (rarely used)
}

// GetTasksProcessed returns the number of tasks processed (atomic)
func (ws *WorkerStats) GetTasksProcessed() int64 {
	return atomic.LoadInt64(&ws.TasksProcessed)
}

// GetTasksFailed returns the number of tasks failed (atomic)
func (ws *WorkerStats) GetTasksFailed() int64 {
	return atomic.LoadInt64(&ws.TasksFailed)
}

// GetTotalTime returns the total processing time (atomic)
func (ws *WorkerStats) GetTotalTime() time.Duration {
	nanos := atomic.LoadInt64(&ws.TotalTime)
	return time.Duration(nanos)
}

// GetActiveTime returns the active processing time (atomic)
func (ws *WorkerStats) GetActiveTime() time.Duration {
	nanos := atomic.LoadInt64(&ws.ActiveTime)
	return time.Duration(nanos)
}

// GetStats returns a snapshot of current stats (atomic reads)
func (ws *WorkerStats) GetStats() (tasksProcessed, tasksFailed int64, totalTime, activeTime time.Duration) {
	return ws.GetTasksProcessed(), ws.GetTasksFailed(), ws.GetTotalTime(), ws.GetActiveTime()
}

// Reset resets all statistics (atomic)
func (ws *WorkerStats) Reset() {
	atomic.StoreInt64(&ws.TasksProcessed, 0)
	atomic.StoreInt64(&ws.TasksFailed, 0)
	atomic.StoreInt64(&ws.TotalTime, 0)
	atomic.StoreInt64(&ws.ActiveTime, 0)
}

// LockFreeQueue represents a lock-free task queue using Go channels
type LockFreeQueue struct {
	tasks   chan Task
	counter int64
}

// NewLockFreeQueue creates a new lock-free queue with buffered capacity
func NewLockFreeQueue(bufferSize int) *LockFreeQueue {
	if bufferSize <= 0 {
		bufferSize = 1000
	}
	return &LockFreeQueue{
		tasks: make(chan Task, bufferSize),
	}
}

// Push adds a task to the queue (non-blocking)
func (q *LockFreeQueue) Push(task Task) bool {
	select {
	case q.tasks <- task:
		atomic.AddInt64(&q.counter, 1)
		return true
	default:
		return false // Queue full
	}
}

// Pop removes and returns a task (blocks until available)
func (q *LockFreeQueue) Pop(ctx context.Context) Task {
	select {
	case task := <-q.tasks:
		atomic.AddInt64(&q.counter, -1)
		return task
	case <-ctx.Done():
		return nil
	}
}

// TryPop attempts to remove a task without blocking
func (q *LockFreeQueue) TryPop() (Task, bool) {
	select {
	case task := <-q.tasks:
		atomic.AddInt64(&q.counter, -1)
		return task, true
	default:
		return nil, false
	}
}

// Size returns the current number of tasks in the queue
func (q *LockFreeQueue) Size() int64 {
	return atomic.LoadInt64(&q.counter)
}

// StealQueue represents a queue that can be stolen from
type StealQueue struct {
	*LockFreeQueue
}

// Steal attempts to steal a task from another worker's queue
func (q *StealQueue) Steal() (Task, bool) {
	return q.TryPop()
}

// WorkStealingScheduler implements a work-stealing scheduler
type WorkStealingScheduler struct {
	workers       []*Worker
	queues        []*StealQueue
	globalQueue   *LockFreeQueue
	resultChan    chan TaskResult
	quit          chan bool
	wg            sync.WaitGroup
	processor     TaskProcessor
	stats         *SchedulerStats
	config        SchedulerConfig
	sharedTicker  *time.Ticker
	tickerSignals []chan struct{} // One signal channel per worker
}

// SchedulerConfig contains configuration for the scheduler
type SchedulerConfig struct {
	NumWorkers         int           // Number of worker goroutines
	QueueSize          int           // Size of per-worker queues
	StealInterval      time.Duration // Interval between steal attempts
	MaxStealAttempts   int           // Maximum steal attempts per interval
	LoadBalance        bool          // Enable load balancing
	MinIdleIntervals   int           // Minimum idle intervals before backoff
	MaxBackoffDuration time.Duration // Maximum backoff duration for idle workers
	EnableStealing     bool          // Enable work stealing (disable for Go scheduler only)
	RebalanceThreshold int           // Minimum queue size difference to trigger rebalance
	RebalanceBatchSize int           // Number of tasks to move at once during rebalance
	RebalanceInterval  time.Duration // Interval between rebalance attempts
	UseGlobalQueue     bool          // Use global queue (false = stealing-only distribution)
	GlobalQueueSize    int           // Size of global queue buffer if enabled
	TaskBatchSize      int           // Number of tasks to process in one batch
	UseSharedTicker    bool          // Use shared ticker instead of per-worker tickers
	TickerInterval     time.Duration // Shared ticker interval
	IdleCheckThreshold int           // Number of iterations before checking steal/backoff
}

// DefaultSchedulerConfig returns a default configuration optimized for low context switches
func DefaultSchedulerConfig() SchedulerConfig {
	return SchedulerConfig{
		NumWorkers:         runtime.NumCPU(),
		QueueSize:          1000,
		StealInterval:      50 * time.Millisecond, // Increased from 10ms to reduce overhead
		MaxStealAttempts:   2,                    // Reduced from 3
		LoadBalance:        false,                // Disabled by default - Go scheduler handles fairness
		MinIdleIntervals:   5,                    // Start backoff after 5 idle intervals
		MaxBackoffDuration: 1 * time.Second,      // Max 1 second backoff
		EnableStealing:     true,                 // Enable stealing as primary distribution
		RebalanceThreshold: 10,                   // Only rebalance if 10+ task difference
		RebalanceBatchSize: 5,                    // Move 5 tasks at once
		RebalanceInterval:  500 * time.Millisecond, // Rebalance every 500ms (not 10ms!)
		UseGlobalQueue:     false,                // Disabled - use stealing-only distribution
		GlobalQueueSize:    0,                    // Not used when disabled
		TaskBatchSize:      10,                   // Process 10 tasks per batch to reduce context switches
		UseSharedTicker:    true,                 // Use shared ticker to reduce context switches
		TickerInterval:     100 * time.Millisecond, // Shared ticker interval
		IdleCheckThreshold: 100,                  // Check steal/backoff every 100 iterations
	}
}

// GoOnlySchedulerConfig returns a configuration that relies entirely on Go's scheduler
func GoOnlySchedulerConfig() SchedulerConfig {
	return SchedulerConfig{
		NumWorkers:         runtime.NumCPU(),
		QueueSize:          10000,                // Large queue for Go scheduler
		StealInterval:      0,                    // Not used
		MaxStealAttempts:   0,                    // Not used
		LoadBalance:        false,                // Disabled - Go handles this
		MinIdleIntervals:   0,                    // Not used
		MaxBackoffDuration: 0,                    // Not used
		EnableStealing:     false,                // No manual stealing
		RebalanceThreshold: 0,                    // Not used
		RebalanceBatchSize: 0,                    // Not used
		RebalanceInterval:  0,                    // Not used
		UseGlobalQueue:     false,                // Not needed for Go-only mode
		GlobalQueueSize:    0,                    // Not used
		TaskBatchSize:      20,                   // Larger batches for Go-only mode
		UseSharedTicker:    false,                // Not needed in Go-only mode
		TickerInterval:     0,                    // Not used
		IdleCheckThreshold: 0,                    // Not used
	}
}

// StealingOnlyConfig returns a configuration optimized for stealing-based distribution
func StealingOnlyConfig() SchedulerConfig {
	return SchedulerConfig{
		NumWorkers:         runtime.NumCPU(),
		QueueSize:          2000,                 // Larger per-worker queues for better distribution
		StealInterval:      25 * time.Millisecond, // Faster stealing for better distribution
		MaxStealAttempts:   3,                    // More attempts when using stealing-only
		LoadBalance:        false,                // Let stealing handle distribution
		MinIdleIntervals:   2,                    // Lower threshold for more responsive stealing
		MaxBackoffDuration: 500 * time.Millisecond, // Shorter backoff
		EnableStealing:     true,                 // Stealing is primary mechanism
		RebalanceThreshold: 0,                    // Not needed when stealing-only
		RebalanceBatchSize: 0,                    // Not needed when stealing-only
		RebalanceInterval:  0,                    // Not needed when stealing-only
		UseGlobalQueue:     false,                // No global queue contention
		GlobalQueueSize:    0,                    // Not used
		TaskBatchSize:      15,                   // Medium batch size for stealing mode
		UseSharedTicker:    true,                 // Use shared ticker to reduce context switches
		TickerInterval:     50 * time.Millisecond, // Faster ticker for stealing mode
		IdleCheckThreshold: 50,                   // Check more frequently in stealing mode
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
	if config.GlobalQueueSize <= 0 && config.UseGlobalQueue {
		config.GlobalQueueSize = config.NumWorkers * 100 // Scale with workers
	}

	scheduler := &WorkStealingScheduler{
		processor:     processor,
		resultChan:    make(chan TaskResult, config.NumWorkers*10),
		quit:          make(chan bool),
		config:        config,
		stats:         &SchedulerStats{StartTime: time.Now()},
		workers:       make([]*Worker, config.NumWorkers),
		queues:        make([]*StealQueue, config.NumWorkers),
		tickerSignals: make([]chan struct{}, config.NumWorkers),
	}

	// Only create global queue if enabled
	if config.UseGlobalQueue {
		scheduler.globalQueue = NewLockFreeQueue(config.GlobalQueueSize)
	}

	// Create shared ticker if enabled
	if config.UseSharedTicker && config.TickerInterval > 0 {
		scheduler.sharedTicker = time.NewTicker(config.TickerInterval)
	}

	// Create workers and their queues
	for i := 0; i < config.NumWorkers; i++ {
		queue := &StealQueue{NewLockFreeQueue(config.QueueSize)}
		scheduler.queues[i] = queue

		// Create ticker signal channel for shared ticker
		var tickerChan chan struct{}
		if config.UseSharedTicker {
			tickerChan = make(chan struct{}, 1) // Buffered to prevent blocking
			scheduler.tickerSignals[i] = tickerChan
		}

		worker := &Worker{
			id:               i,
			resultChan:       scheduler.resultChan,
			quit:             make(chan bool),
			processor:        processor,
			stats:            &WorkerStats{},
			idleIntervals:    0,
			backoffDuration:  0,
			iterationCounter: 0,
			tickerSignal:     tickerChan,
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

	// Start shared ticker broadcaster if enabled
	if s.config.UseSharedTicker && s.sharedTicker != nil {
		s.wg.Add(1)
		go s.runSharedTickerBroadcaster(ctx)
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

	// Stop shared ticker
	if s.sharedTicker != nil {
		s.sharedTicker.Stop()
	}

	s.wg.Wait()
	close(s.resultChan)
}

// Submit submits a task to the scheduler
func (s *WorkStealingScheduler) Submit(task Task) error {
	atomic.AddInt64(&s.stats.TasksSubmitted, 1)

	// If global queue is disabled, use direct worker submission
	if !s.config.UseGlobalQueue {
		return s.submitToWorkerDirect(task)
	}

	// Try to submit to a worker queue directly if load balancing is enabled
	if s.config.LoadBalance {
		// Find the least loaded worker
		minQueueSize := int64(^uint(0) >> 1) // Max int64
		targetWorker := 0

		for i, queue := range s.queues {
			queueSize := queue.Size()
			if queueSize < minQueueSize {
				minQueueSize = queueSize
				targetWorker = i
			}
		}

		// Submit to the least loaded worker if queue accepts the task
		if s.queues[targetWorker].Push(task) {
			return nil
		}
	}

	// Submit to global queue as fallback
	if s.globalQueue != nil && s.globalQueue.Push(task) {
		return nil
	}

	return fmt.Errorf("all queues are full, task rejected")
}

// submitToWorkerDirect submits a task directly to a worker queue (no global queue)
func (s *WorkStealingScheduler) submitToWorkerDirect(task Task) error {
	// Try round-robin submission to distribute tasks
	for i := 0; i < len(s.queues); i++ {
		if s.queues[i].Push(task) {
			return nil
		}
	}

	// If all queues are full, try stealing-based submission
	if s.config.EnableStealing {
		// Try to find any worker with space by checking all queues
		for _, queue := range s.queues {
			if queue.Push(task) {
				return nil
			}
		}
	}

	return fmt.Errorf("all worker queues are full, task rejected")
}

// runSharedTickerBroadcaster broadcasts ticker signals to all workers (single goroutine, many listeners)
func (s *WorkStealingScheduler) runSharedTickerBroadcaster(ctx context.Context) {
	defer s.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.quit:
			return
		case <-s.sharedTicker.C:
			// Non-blocking broadcast to all workers
			for _, signalChan := range s.tickerSignals {
				select {
				case signalChan <- struct{}{}:
					// Signal sent
				default:
					// Channel already has a signal, skip
				}
			}
		}
	}
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

// runWorker runs a worker goroutine with minimal context switches
func (s *WorkStealingScheduler) runWorker(ctx context.Context, worker *Worker, queue *StealQueue) {
	defer s.wg.Done()
	atomic.AddInt32(&s.stats.ActiveWorkers, 1)
	defer atomic.AddInt32(&s.stats.ActiveWorkers, -1)

	for {
		worker.iterationCounter++

		// Try to get a task from local queue first (non-blocking)
		if task, ok := queue.TryPop(); ok {
			s.processTaskBatch(worker, task, queue)
			worker.idleIntervals = 0      // Reset idle counter
			worker.backoffDuration = 0    // Reset backoff
			continue
		}

		// Check if we should perform expensive operations (stealing, backoff)
		shouldCheckExpensiveOps := s.config.IdleCheckThreshold <= 0 ||
			(worker.iterationCounter % s.config.IdleCheckThreshold) == 0

		if shouldCheckExpensiveOps {
			// Try to steal from other workers (if enabled)
			if s.config.EnableStealing {
				if task := s.stealTask(worker.id); task != nil {
					s.processTaskBatch(worker, task, queue)
					worker.idleIntervals = 0   // Reset idle counter
					worker.backoffDuration = 0 // Reset backoff
					continue
				}
			}

			// Try global queue (only if enabled)
			if s.config.UseGlobalQueue && s.globalQueue != nil {
				if task, ok := s.globalQueue.TryPop(); ok {
					s.processTaskBatch(worker, task, queue)
					worker.idleIntervals = 0      // Reset idle counter
					worker.backoffDuration = 0    // Reset backoff
					continue
				}
			}
		}

		// If all optimization features are disabled, just wait on channel
		if !s.config.EnableStealing && !s.config.LoadBalance {
			// Simple channel-based waiting - let Go scheduler handle everything
			select {
			case <-ctx.Done():
				return
			case <-s.quit:
				return
			case task := <-queue.tasks:
				s.processTaskBatch(worker, task, queue)
				worker.idleIntervals = 0
				worker.backoffDuration = 0
				continue
			}
			continue
		}

		// Handle timing - either shared ticker or per-worker timing
		if s.config.UseSharedTicker && worker.tickerSignal != nil {
			// Wait for shared ticker signal or quit
			select {
			case <-ctx.Done():
				return
			case <-s.quit:
				return
			case <-worker.tickerSignal:
				// Received ticker signal, continue with next iteration
				runtime.Gosched() // Small yield
			}
		} else if shouldCheckExpensiveOps {
			// Only do backoff on expensive operation checks
			s.handleWorkerBackoff(worker, ctx)
		} else {
			// Quick yield without timer for most iterations
			runtime.Gosched()
		}
	}
}

// stealTask attempts to steal a task from another worker using randomized selection
func (s *WorkStealingScheduler) stealTask(workerID int) Task {
	if !s.config.EnableStealing {
		return nil
	}

	numWorkers := len(s.queues)
	if numWorkers <= 1 {
		return nil
	}

	// Create randomized order of workers to try from
	workers := rand.Perm(numWorkers - 1) // Exclude self

	for i := 0; i < s.config.MaxStealAttempts && i < len(workers); i++ {
		// Map randomized index to actual worker ID, skipping self
		targetID := workers[i]
		if targetID >= workerID {
			targetID++
		}

		atomic.AddInt64(&s.stats.StealAttempts, 1)
		if task, ok := s.queues[targetID].Steal(); ok {
			atomic.AddInt64(&s.stats.StealSuccesses, 1)
			return task
		}
	}
	return nil
}

// processTaskBatch processes a single task and tries to batch more tasks from the same queue
func (s *WorkStealingScheduler) processTaskBatch(worker *Worker, initialTask Task, queue *StealQueue) {
	tasks := make([]Task, 0, s.config.TaskBatchSize)
	tasks = append(tasks, initialTask)

	// Try to pull more tasks from the local queue for batch processing
	for len(tasks) < s.config.TaskBatchSize {
		if task, ok := queue.TryPop(); ok {
			tasks = append(tasks, task)
		} else {
			break
		}
	}

	// Process the batch
	for _, task := range tasks {
		s.executeTask(worker, task)
	}
}

// executeTask executes a task and sends the result
func (s *WorkStealingScheduler) executeTask(worker *Worker, task Task) {
	start := time.Now()

	result, err := s.processor.Process(task)

	duration := time.Since(start)
	durationNanos := duration.Nanoseconds()

	// Update worker stats using atomic operations (no locks!)
	atomic.AddInt64(&worker.stats.TasksProcessed, 1)
	atomic.AddInt64(&worker.stats.TotalTime, durationNanos)
	atomic.AddInt64(&worker.stats.ActiveTime, durationNanos)
	if err != nil {
		atomic.AddInt64(&worker.stats.TasksFailed, 1)
	}

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

// handleWorkerBackoff handles backoff timing for idle workers
func (s *WorkStealingScheduler) handleWorkerBackoff(worker *Worker, ctx context.Context) {
	worker.idleIntervals++

	// Calculate backoff duration if we've been idle long enough
	if worker.idleIntervals >= s.config.MinIdleIntervals {
		if worker.backoffDuration == 0 {
			worker.backoffDuration = s.config.StealInterval
		} else {
			// Exponential backoff with jitter
			worker.backoffDuration = time.Duration(float64(worker.backoffDuration) * 1.5)
			if worker.backoffDuration > s.config.MaxBackoffDuration {
				worker.backoffDuration = s.config.MaxBackoffDuration
			}
			// Add jitter to prevent thundering herd
			jitter := time.Duration(rand.Float64() * float64(worker.backoffDuration) * 0.1)
			worker.backoffDuration += jitter
		}
	} else {
		worker.backoffDuration = s.config.StealInterval
	}

	// Wait for work or backoff timeout
	backoffTimer := time.NewTimer(worker.backoffDuration)
	select {
	case <-ctx.Done():
		backoffTimer.Stop()
		return
	case <-s.quit:
		backoffTimer.Stop()
		return
	case <-backoffTimer.C:
		// Backoff completed, try again
		runtime.Gosched() // Yield to other goroutines
	}
}

// runLoadBalancer runs the load balancer with intelligent scheduling
func (s *WorkStealingScheduler) runLoadBalancer(ctx context.Context) {
	defer s.wg.Done()

	// Use rebalance interval instead of steal interval for less frequent checks
	ticker := time.NewTicker(s.config.RebalanceInterval)
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

// balanceLoad balances tasks between workers using threshold-based approach
func (s *WorkStealingScheduler) balanceLoad() {
	if len(s.queues) < 2 {
		return
	}

	// Find workers with most and least tasks
	maxTasks := int64(0)
	minTasks := int64(^uint(0) >> 1) // Max int64
	maxWorker := 0
	minWorker := 0

	// Scan all queues to find extremes
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

	// Only rebalance if the difference exceeds the threshold
	if maxTasks-minTasks >= int64(s.config.RebalanceThreshold) && maxTasks > 0 {
		s.rebalanceTasks(maxWorker, minWorker, maxTasks, minTasks)
	}
}

// rebalanceTasks moves multiple tasks from overloaded to underloaded worker
func (s *WorkStealingScheduler) rebalanceTasks(fromWorker, toWorker int, maxTasks, minTasks int64) {
	// Calculate how many tasks to move
	taskDiff := maxTasks - minTasks
	tasksToMove := int64(s.config.RebalanceBatchSize)

	// Don't move more than half the difference or the batch size
	if tasksToMove > taskDiff/2 {
		tasksToMove = taskDiff / 2
	}

	moved := 0
	for i := int64(0); i < tasksToMove && moved < s.config.RebalanceBatchSize; i++ {
		if task, ok := s.queues[fromWorker].Steal(); ok {
			// Try to push to the underloaded worker
			if !s.queues[toWorker].Push(task) {
				// If target is full, try global queue if available, otherwise discard
				if s.config.UseGlobalQueue && s.globalQueue != nil {
					s.globalQueue.Push(task)
				}
				// If no global queue, task is discarded (stealing system will redistribute)
			} else {
				moved++
			}
		} else {
			// No more tasks to steal
			break
		}
	}

	// Optional: Log rebalancing for debugging (would need proper logging)
	// if moved > 0 {
	//     fmt.Printf("Rebalanced %d tasks from worker %d to worker %d\n", moved, fromWorker, toWorker)
	// }
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

// GoSchedulerSimple implements a simple scheduler that relies entirely on Go's runtime scheduler
type GoSchedulerSimple struct {
	taskChan    chan Task
	resultChan  chan TaskResult
	processor   TaskProcessor
	quit        chan bool
	wg          sync.WaitGroup
	stats       *SchedulerStats
	numWorkers  int
}

// NewGoSchedulerSimple creates a simple Go scheduler-only processor
func NewGoSchedulerSimple(processor TaskProcessor, numWorkers int, queueSize int) *GoSchedulerSimple {
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}
	if queueSize <= 0 {
		queueSize = 10000
	}

	return &GoSchedulerSimple{
		taskChan:   make(chan Task, queueSize),
		resultChan: make(chan TaskResult, numWorkers*10),
		quit:       make(chan bool),
		processor:  processor,
		stats:      &SchedulerStats{StartTime: time.Now()},
		numWorkers: numWorkers,
	}
}

// Start starts the simple scheduler workers
func (s *GoSchedulerSimple) Start(ctx context.Context) {
	for i := 0; i < s.numWorkers; i++ {
		s.wg.Add(1)
		go s.worker(ctx)
	}
}

// Stop stops the scheduler
func (s *GoSchedulerSimple) Stop() {
	close(s.quit)
	s.wg.Wait()
	close(s.resultChan)
}

// Submit submits a task to the scheduler
func (s *GoSchedulerSimple) Submit(task Task) error {
	atomic.AddInt64(&s.stats.TasksSubmitted, 1)

	select {
	case s.taskChan <- task:
		return nil
	case <-s.quit:
		return fmt.Errorf("scheduler is shutting down")
	default:
		return fmt.Errorf("task queue is full")
	}
}

// Results returns the result channel
func (s *GoSchedulerSimple) Results() <-chan TaskResult {
	return s.resultChan
}

// Stats returns scheduler statistics
func (s *GoSchedulerSimple) Stats() SchedulerStats {
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

// worker processes tasks from the channel
func (s *GoSchedulerSimple) worker(ctx context.Context) {
	defer s.wg.Done()
	atomic.AddInt32(&s.stats.ActiveWorkers, 1)
	defer atomic.AddInt32(&s.stats.ActiveWorkers, -1)

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.quit:
			return
		case task := <-s.taskChan:
			result, err := s.processor.Process(task)

			// Update stats
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
	}
}
