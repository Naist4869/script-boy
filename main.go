package main

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	_ "go.uber.org/automaxprocs"
)

var (
	nextParseLine            int
	qps                      int
	parallel                 int
	limit                    int64
	timeout                  int64
	inputExample             string
	outputExample            string
	userPassDelimiter        string
	passAccessTokenDelimiter string
)

func init() {
	// 设置命令行标志
	rootCmd.PersistentFlags().IntVar(&nextParseLine, "nextParseLine", 0, "Next parse line number")
	rootCmd.PersistentFlags().IntVar(&qps, "qps", 1, "Queries per second limit")
	rootCmd.PersistentFlags().IntVar(&parallel, "parallel", 0, "Number of parallel workers")
	rootCmd.PersistentFlags().Int64Var(&limit, "limit", 0, "Account Limit")
	rootCmd.PersistentFlags().Int64Var(&timeout, "timeout", 0, "Timeout in seconds")
	rootCmd.PersistentFlags().StringVar(&inputExample, "inputExample", "matt.carpenter1411@gmail.com:Carpie14", "Input example like matt.carpenter1411@gmail.com:Carpie14")
	rootCmd.PersistentFlags().StringVar(&outputExample, "outputExample", "matt.carpenter1411@gmail.com:Carpie14 | ATK = <accesstoken>", "Output example like matt.carpenter1411@gmail.com:Carpie14 | ATK = <accesstoken>")

	userPassDelimiter, passAccessTokenDelimiter = parseDelimiter(inputExample, outputExample)
}

var rootCmd = &cobra.Command{
	Use:   "transform [input file] [output file]",
	Short: "Transform appends a specified string to each line of the input file",
	Args:  cobra.RangeArgs(1, 2), // 修改这里
	Run:   transform,
}

func findDelimiter(s string) string {
	delimiter := ""
	for _, char := range s {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '@' || char == '.' {
			if delimiter != "" {
				break
			}
		} else {
			delimiter += string(char)
		}
	}
	return delimiter
}

func parseDelimiter(inputStr, outputStr string) (string, string) {
	userPassDelimiter := findDelimiter(inputStr)

	// Identifying the delimiter between password and accesstoken in the output string
	startIndex := strings.Index(outputStr, inputStr) + len(inputStr)

	remainingStr := outputStr[startIndex:]
	endIndex := strings.Index(remainingStr, "<")
	passAccessTokenDelimiter := remainingStr[:endIndex]

	return userPassDelimiter, passAccessTokenDelimiter
}

func transform(cmd *cobra.Command, args []string) {
	inputFileName := args[0]
	outputFileName := ""

	if len(args) == 2 {
		outputFileName = args[1] // 如果提供了输出文件名，则使用它
	} else {
		ext := filepath.Ext(inputFileName)                 // 获取文件扩展名
		baseName := strings.TrimSuffix(inputFileName, ext) // 获取不包含扩展名的文件名
		outputFileName = baseName + "_out" + ext           // 构建输出文件名
	}

	entries, nextParseLineTemp, outputFile, err := openFile(inputFileName, outputFileName)
	if err != nil {
		slog.Error(fmt.Sprintf("Error opening or creating output file: %v", err))
		return
	}
	if nextParseLine == 0 {
		nextParseLine = nextParseLineTemp
	}
	defer outputFile.Close()
	ctx, cancelFunc := context.WithCancelCause(context.Background())
	c, err := newCoordinator(ctx, cancelFunc, outputFile, entries, nextParseLine, qps, parallel, limit, timeout)
	if err != nil {
		slog.Error(fmt.Sprintf("Error creating coordinator: %v", err))
		return
	}
	errChan := make(chan error, 1)

	go func() {
		err := c.CoordinateStart()
		errChan <- err
	}()

	// listen for interrupt signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	// 等待中断信号或 CoordinateStart 完成
	for {
		select {
		case <-ch:
			slog.Warn("Interrupt received. Waiting for tasks to complete.")
			cancelFunc(errors.New("interrupt"))
			// 接收到中断信号，但不立即退出
		case err := <-errChan:
			if err != nil {
				slog.Info(fmt.Sprintf("coordinateStart: %v", err))
			}
			// 无论是否接收到中断信号，都在此处退出循环
			goto END
		}
	}

END:
	slog.Info("Exiting program.")
}

func openFile(inputFileName string, outputFileName string) ([]entry, int, *os.File, error) {

	outputFile, err := os.OpenFile(outputFileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, 0, nil, errors.Wrap(err, "opening or creating output file")
	}

	// 计算需要跳过的行数
	skipLine, err := calculateSkipLine(outputFile)
	if err != nil {
		outputFile.Close()
		return nil, 0, nil, errors.Wrap(err, "reading from output file")
	}

	inputFile, err := os.Open(inputFileName)
	if err != nil {
		outputFile.Close()
		return nil, 0, nil, errors.Wrap(err, "opening input file")
	}
	defer inputFile.Close()

	entries, nextParseLine, err := processInputFile(inputFile, outputFile, skipLine)
	if err != nil {
		outputFile.Close()
		return nil, 0, nil, err
	}

	return entries, nextParseLine, outputFile, nil
}

func calculateSkipLine(file *os.File) (int, error) {
	scanner := bufio.NewScanner(file)
	lineCount := 0
	for scanner.Scan() {
		lineCount++
	}

	return lineCount + 1, scanner.Err()
}

func processInputFile(inputFile *os.File, outputFile *os.File, skipLine int) ([]entry, int, error) {
	var entries = []entry{{}}
	scanner := bufio.NewScanner(inputFile)
	lineCount := 0

	var nextParseLine int
	for scanner.Scan() {
		line := scanner.Text()
		lineCount++

		var (
			username string
			password string
		)

		if lineCount >= skipLine {

			if strings.Contains(line, "@") && len(strings.Split(line, userPassDelimiter)) > 1 {
				if nextParseLine == 0 {
					nextParseLine = lineCount
				}
				split := strings.Split(line, userPassDelimiter)
				if len(split) < 2 {
					slog.Error(fmt.Sprintf("invalid line: %s", line))
				}
				username = split[0]
				password = split[1]
			} else {
				if _, err := outputFile.WriteString(line + "\n"); err != nil {
					return nil, 0, errors.Wrap(err, "writing to output file")
				}
			}

		}

		entries = append(entries, entry{
			username: username,
			password: password,
			line:     lineCount,
		})
	}

	return entries, nextParseLine, scanner.Err()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
