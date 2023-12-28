package main

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
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
	passAccessTokenDelimiter string
	onlyATK                  bool
	endpoint                 string
	delimiterInput           string
	delimiters               []string // 解析后的分隔符列表

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
	rootCmd.PersistentFlags().BoolVar(&onlyATK, "onlyATK", false, "Output only the ATK if set to true")
	rootCmd.PersistentFlags().StringVar(&endpoint, "endpoint", "https://replace-your-url/api/auth", "provider your endpoint")
	rootCmd.PersistentFlags().StringVar(&delimiterInput, "delimiters", "", "Comma-separated list of custom delimiters for parsing")
	delimiters = strings.Split(delimiterInput, ",") // 使用逗号分隔用户输入的字符串

	passAccessTokenDelimiter = parseDelimiter(inputExample, outputExample)
}

var rootCmd = &cobra.Command{
	Use:   "transform [input file] [output file]",
	Short: "Transform appends a specified string to each line of the input file",
	Args:  cobra.RangeArgs(1, 2), // 修改这里
	Run:   transform,
}

func findDelimiter(s string) string {
	for _, delimiter := range delimiters {
		if strings.Contains(s, delimiter) {
			return delimiter
		}
	}
	return ""
}

func parseDelimiter(inputStr, outputStr string) string {
	// Identifying the delimiter between password and accesstoken in the output string
	startIndex := strings.Index(outputStr, inputStr) + len(inputStr)

	remainingStr := outputStr[startIndex:]
	endIndex := strings.Index(remainingStr, "<")
	passAccessTokenDelimiter := remainingStr[:endIndex]

	return passAccessTokenDelimiter
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
	skipLine, breakpointResumption, err := calculateSkipLine(outputFile)
	if err != nil {
		outputFile.Close()
		return nil, 0, nil, errors.Wrap(err, "reading from output file")
	}

	inputFile, err := os.Open(inputFileName)
	if err != nil {
		outputFile.Close()
		os.Remove(outputFileName) // 删除文件并忽略错误
		return nil, 0, nil, errors.Wrap(err, "opening input file")
	}
	defer inputFile.Close()

	entries, nextParseLine, err := processInputFile(inputFile, outputFile, skipLine, breakpointResumption)
	if err != nil {
		outputFile.Close()
		return nil, 0, nil, err
	}

	return entries, nextParseLine, outputFile, nil
}

func calculateSkipLine(file *os.File) (string, bool, error) {
	scanner := bufio.NewScanner(file)
	var (
		breakpointResumption bool
		lastLine             string
	)
	for scanner.Scan() {
		breakpointResumption = true
		line := scanner.Text()

		if strings.Contains(line, "@") {
			lastLine = line
		}
	}

	if breakpointResumption && lastLine == "" {
		// Clear the file content
		err := file.Truncate(0)
		if err != nil {
			return "", false, err // Return the error if truncation fails
		}
		// Seek to the beginning of the file
		_, err = file.Seek(0, 0)
		if err != nil {
			return "", false, err // Return the error if seeking fails
		}
		return "", false, nil
	}

	return lastLine, breakpointResumption, scanner.Err()
}

var userPassDelimiterMap = map[string]struct{}{}

var userPassDelimiterReMap = map[string]*regexp.Regexp{}
var oldnew []string

func processInputFile(inputFile *os.File, outputFile *os.File, skipLine string, breakpointResumption bool) ([]entry, int, error) {
	var entries = []entry{{}}
	scanner := bufio.NewScanner(inputFile)

	var (
		nextParseLine           int
		parsedHeaderInformation bool
	)

	for lineCount := 1; scanner.Scan(); lineCount++ {
		line := scanner.Text()

		// Step 1: Normalize spaces around delimiters
		for delimiter, re := range userPassDelimiterReMap {
			line = re.ReplaceAllString(line, " "+delimiter+" ")
		}

		// Step 2: Apply delimiter formatting
		replacer := strings.NewReplacer(oldnew...)
		line = replacer.Replace(line)
		parts := strings.SplitN(line, " ", 2)
		firstPart := parts[0]
		var (
			username string
			password string
		)

		delimiter := findDelimiter(line)
		if breakpointResumption {
			if nextParseLine == 0 {
				nextParseLine = BreakpointResumption(skipLine, firstPart, lineCount)
			}
			if nextParseLine != 0 && lineCount >= nextParseLine {
				if strings.Contains(firstPart, "@") && strings.Contains(firstPart, ".") && delimiter != "" {
					userPassDelimiter := delimiter
					_, exist := userPassDelimiterMap[userPassDelimiter]
					if !exist {
						userPassDelimiterMap[userPassDelimiter] = struct{}{}
						regexPattern := "\\s*" + regexp.QuoteMeta(delimiter) + "\\s*"
						re := regexp.MustCompile(regexPattern)
						userPassDelimiterReMap[userPassDelimiter] = re

						oldnew = append(oldnew, " "+delimiter+" ", delimiter)
						oldnew = append(oldnew, delimiter+" ", delimiter)
						oldnew = append(oldnew, " "+delimiter, delimiter)

					}

					split := strings.Split(firstPart, userPassDelimiter)
					if len(split) >= 2 {
						username = split[0]
						password = strings.Join(split[1:], userPassDelimiter)
					} else {
						slog.Error(fmt.Sprintf("skip %d, invalid line: %s, breakpointResumption: %t", lineCount, line, breakpointResumption))
					}
				} else {
					slog.Error(fmt.Sprintf("skip %d, invalid line: %s, breakpointResumption: %t", lineCount, line, breakpointResumption))
				}
			}
		} else {
			if strings.Contains(firstPart, "@") && strings.Contains(firstPart, ".") {
				parsedHeaderInformation = true
				userPassDelimiter := delimiter
				_, exist := userPassDelimiterMap[userPassDelimiter]
				if !exist {
					userPassDelimiterMap[userPassDelimiter] = struct{}{}
					regexPattern := "\\s*" + regexp.QuoteMeta(delimiter) + "\\s*"
					re := regexp.MustCompile(regexPattern)
					userPassDelimiterReMap[userPassDelimiter] = re
					oldnew = append(oldnew, " "+delimiter+" ", delimiter)
					oldnew = append(oldnew, delimiter+" ", delimiter)
					oldnew = append(oldnew, " "+delimiter, delimiter)
				}
				split := strings.Split(firstPart, userPassDelimiter)
				if len(split) >= 2 {
					if nextParseLine == 0 {
						nextParseLine = lineCount
					}
					username = split[0]
					password = strings.Join(split[1:], userPassDelimiter)
				} else {
					slog.Error(fmt.Sprintf("skip %d, invalid line: %s, breakpointResumption: %t", lineCount, line, breakpointResumption))
				}
			} else if !parsedHeaderInformation {
				if _, err := outputFile.WriteString(line + "\n"); err != nil {
					return nil, 0, errors.Wrap(err, "writing to output file")
				}
			} else {
				slog.Error(fmt.Sprintf("skip %d, invalid line: %s, breakpointResumption: %t", lineCount, line, breakpointResumption))
			}
		}

		entries = append(entries, entry{
			username:          username,
			password:          password,
			line:              lineCount,
			userPassDelimiter: delimiter,
		})
	}

	return entries, nextParseLine, scanner.Err()
}

func BreakpointResumption(skipLine string, line string, lineCount int) int {
	if line != "" && strings.Contains(skipLine, line) {
		return lineCount + 1
	}
	return 0
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
