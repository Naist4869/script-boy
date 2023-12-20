package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

type coordinator struct {
	ctx          context.Context
	cancel       context.CancelCauseFunc
	parallel     int
	limit        int64
	timeout      int64
	count        int64         // 执行任务数
	countWaiting int64         // 等待任务数
	countLastLog int64         // 报告任务数
	timeLastLog  time.Time     // 最后一次日志时间
	startTime    time.Time     // 用于记录任务开始时间
	duration     time.Duration // 运行时间
	limiter      *rate.Limiter
	outputFile   *os.File

	inputC        chan input  // 用于发送任务
	resultC       chan result // 用于接收任务结果
	queue         queue
	entries       []entry // 所有行号
	flushLine     int
	nextParseLine int
	group         int
}

type input struct {
	entry
	limit int64
}

type result struct {
	count         int64
	limit         int64
	totalDuration time.Duration
	workResult    entry
	err           error
}

type entry struct {
	username    string
	password    string
	accessToken string
	line        int
	processed   bool
}

func newCoordinator(ctx context.Context, cancel context.CancelCauseFunc, outputFile *os.File, entries []entry, nextParseLine int, qps int, parallel int, limit int64, timeout int64) (*coordinator, error) {
	var limiter *rate.Limiter
	if qps == 0 {
		qps = 1
	}

	limiter = rate.NewLimiter(rate.Limit(qps), qps)
	if nextParseLine <= 0 {
		nextParseLine = 1
	}
	return &coordinator{
		ctx:           ctx,
		cancel:        cancel,
		parallel:      parallel,
		limit:         limit,
		timeout:       timeout,
		timeLastLog:   time.Now(),
		startTime:     time.Now(),
		inputC:        make(chan input),
		resultC:       make(chan result),
		group:         qps * 60,
		limiter:       limiter,
		entries:       entries,
		outputFile:    outputFile,
		flushLine:     nextParseLine,
		nextParseLine: nextParseLine,
	}, nil
}

func (c *coordinator) CoordinateStart() (err error) {
	if c.parallel == 0 {
		c.parallel = runtime.GOMAXPROCS(0)
	}

	if c.limit > 0 && int64(c.parallel) > c.limit {
		// 任务数量没那么多的时候避免启动太多worker处理
		c.parallel = int(c.limit)
	}

	if c.timeout > 0 {
		var cancel func()
		c.ctx, cancel = context.WithTimeout(c.ctx, time.Duration(c.timeout)*time.Second)
		defer cancel()
	}

	workerCtx, cancelWorkers := context.WithCancel(c.ctx)
	defer cancelWorkers()

	doneC := c.ctx.Done()
	var workerErr error

	stopping := false
	stop := func(err error) {
		if errors.Is(err, workerCtx.Err()) {
			err = nil
		}

		if err != nil && (workerErr == nil || errors.Is(c.ctx.Err(), workerErr)) {
			workerErr = err
		}

		if stopping {
			return
		}
		stopping = true
		cancelWorkers()
		// 不会被select到
		doneC = nil

	}

	errC := make(chan error)
	workers := make([]*worker, c.parallel)
	for i := range workers {
		var err error
		workers[i], err = newWorker(c)
		if err != nil {
			return err
		}
	}

	for i := range workers {
		w := workers[i]
		go func() {
			defer func() {
				if r := recover(); r != nil {
					err := fmt.Errorf("work goroutine panic: %v", r)
					errC <- err
					slog.Error(err.Error())
				}
			}()
			err := w.coordinate(workerCtx)
			if workerCtx.Err() != nil {
				err = nil
			}
			errC <- err
		}()
	}

	// 主goroutine
	activeWorkers := len(workers)
	// 日志报告间隔
	statTicker := time.NewTicker(3 * time.Second)
	defer statTicker.Stop()

	flushTicker := time.NewTicker(2 * time.Second)
	defer flushTicker.Stop()
	// 停止时打印一次log
	defer c.logStats(stopping, true)
	// 开始时打印一次log
	c.logStats(stopping, false)

	for {
		var inputC chan input
		input, ok, done := c.peekInput()
		if ok && !stopping && !done {
			// nil的chan不会被select到 需要赋值
			inputC = c.inputC
		}

		select {
		case <-doneC:
			stop(c.ctx.Err())
		case err := <-errC:
			stop(err)
			activeWorkers--
			if activeWorkers == 0 {
				return workerErr
			}
		case result := <-c.resultC:
			//if stopping {
			//	break
			//}
			c.updateStats(result)

			c.entries[result.workResult.line] = result.workResult
			if result.err != nil {
				slog.Info(fmt.Sprintf("line: %d, err: %v", result.workResult.line, result.err))
			} else {
				slog.Info(fmt.Sprintf("line: %d, ok", result.workResult.line))
			}

			if c.limit > 0 && c.count >= c.limit {
				stop(nil)
			}
		case inputC <- input:
			c.sentInput(input)
		case <-statTicker.C:
			c.logStats(stopping, false)
		case <-flushTicker.C:
			c.writeLines(c.entries[c.flushLine:c.nextParseLine])
		default:
			time.Sleep(time.Millisecond)
		}
		if done && c.countWaiting == 0 {
			return context.Cause(c.ctx)
		}
	}
}

func (c *coordinator) updateStats(result result) {
	c.count += result.count
	c.countWaiting -= result.limit
	c.duration += result.totalDuration
}

func (c *coordinator) peekInput() (input, bool, bool) {
	if c.limit > 0 && c.count+c.countWaiting >= c.limit {
		return input{}, false, false
	}

	if c.limiter != nil && !c.limiter.Allow() {
		// 如果达到了速率限制，则返回 false
		return input{}, false, false
	}

	if c.queue.len == 0 {
		c.refillInputQueue()
	}

	e, ok := c.queue.peek()
	if !ok {
		return input{}, false, true
	}

	input := input{
		entry: e.(entry),
		limit: 1,
	}

	return input, true, false
}

func (c *coordinator) sentInput(input input) {
	c.queue.dequeue()
	c.countWaiting += input.limit
}

func (c *coordinator) logStats(stopping, stopped bool) {
	// 获取当前执行到的行数
	now := time.Now()
	r := float64(c.count-c.countLastLog) / now.Sub(c.timeLastLog).Minutes()
	progress := float64(c.count) * 100 / float64(c.nextParseLine-1)

	slog.Info(fmt.Sprintf("next parsing line: %d, execution time: %s, number of successful executions: %d, number awaiting: %d, task completion rate: %f%%, speed: %.0f/min, total runtime of workers: %s, is stopping: %t, process has exited: %t", c.nextParseLine, c.elapsed(), c.count, c.countWaiting, progress, r, c.duration, stopping, stopped))

	c.countLastLog = c.count
	c.timeLastLog = now
}

func (c *coordinator) elapsed() time.Duration {
	return time.Since(c.startTime).Round(1 * time.Second)
}

func (c *coordinator) refillInputQueue() {

	if c.nextParseLine+c.group <= len(c.entries) {
		for i := 0; i < c.group; i++ {
			c.queue.enqueue(c.entries[c.nextParseLine+i])
		}

		c.nextParseLine = c.nextParseLine + c.group
	}

}

func (c *coordinator) writeLines(entries []entry) error {
	if len(entries) == 0 {
		return nil // 没有条目时直接返回
	}

	w := bufio.NewWriter(c.outputFile)
	defer w.Flush() // 延迟刷新以确保所有数据都被写入

	var count int
	for i, line := range entries {
		// 对于第一行或者前一行已处理的行进行写入
		if i == 0 || entries[i-1].processed {
			if line.processed {
				count = i + 1

				if line.accessToken != "" {
					if index := strings.Index(line.accessToken, "<"); index == -1 {
						if _, err := fmt.Fprintf(w, "%s%s%s%s%s\n", line.username, userPassDelimiter, line.password, passAccessTokenDelimiter, line.accessToken); err != nil {
							return err // 处理写入错误
						}
					}
				}

			} else {
				break // 遇到未处理的行，停止写入
			}
		}
	}
	c.flushLine += count
	return nil
}
