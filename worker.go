package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/avast/retry-go"

	"github.com/pkg/errors"
)

type worker struct {
	coordinator *coordinator
	client      *http.Client
}

func newWorker(c *coordinator) (*worker, error) {
	return &worker{
		coordinator: c,
		client: &http.Client{
			Timeout: 120 * time.Second,
		},
	}, nil
}

func (w *worker) coordinate(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			// 被告知停止
			return ctx.Err()
		case input := <-w.coordinator.inputC:
			start := time.Now()
			workResult, err := w.Work(input)
			totalDuration := time.Since(start)
			result := result{
				count:         1,
				limit:         input.limit,
				totalDuration: totalDuration,
				workResult:    workResult,
				err:           err,
			}
			w.coordinator.resultC <- result
		}
	}
}

type Response struct {
	ErrorMsg string             `json:"error_msg"`
	Code     int32              `json:"code"`
	Reason   string             `json:"reason"`
	Message  string             `json:"message"`
	Metadata map[string]string  `json:"metadata"`
	Data     GetSessionResponse `json:"data"`
}

type GetSessionResponse struct {
	AccessToken   string    `json:"access_token"`
	Expires       time.Time `json:"expires"`
	RefreshCookie string    `json:"refresh_cookie"`
}

var ErrRetry = errors.New("need retry")

func (w *worker) Work(input input) (entry, error) {
	if input.entry.username == "" || input.entry.password == "" {
		return input.entry, nil
	}
	slog.Info(fmt.Sprintf("handling work line: %d", input.line))
	entry := input.entry
	entry.processed = true

	err := retry.Do(func() error {
		url := "https://replace-your-url/api/auth"
		payload := strings.NewReader(fmt.Sprintf("username=%s&password=%s", entry.username, entry.password))
		req, err := http.NewRequest("POST", url, payload)
		if err != nil {
			return err
		}
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		response, err := w.client.Do(req)
		if err != nil {
			entry.accessToken = "<" + err.Error() + ">"
			return errors.Wrap(ErrRetry, err.Error())
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return err
		}
		v := new(Response)
		if err = json.Unmarshal(body, v); err != nil {
			return err
		}

		if v.ErrorMsg != "" {
			return errors.Wrap(ErrRetry, v.ErrorMsg)
		}

		if v.Code != 0 {
			if v.Reason == "" {
				entry.accessToken = "<" + v.Message + ">"
			} else {
				entry.accessToken = "<" + v.Reason + ">"
				if v.Reason == "INTERNAL_SERVER_ERROR" {
					return errors.Wrap(ErrRetry, v.Reason)
				}
			}
			return fmt.Errorf("code: %d, reason: %s, message: %s", v.Code, v.Reason, v.Message)
		}
		entry.accessToken = v.Data.AccessToken
		return nil
	},
		retry.Attempts(5),
		retry.Delay(300*time.Millisecond),
		retry.OnRetry(func(n uint, err error) {
			//slog.Warn(fmt.Sprintf("Retry line: %d, #%d: %v", entry.line, n, err))
		}),
		retry.RetryIf(func(err error) bool {
			return errors.Is(err, ErrRetry)
		},
		))
	slog.Info(fmt.Sprintf("success work line: %d", entry.line))
	if err != nil {
		return entry, err
	}

	return entry, nil
}
