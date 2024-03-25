package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
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
var emailFile, dbFile, outputFile string

func init() {
	// 设置命令行标志
	transformCmd.PersistentFlags().IntVar(&nextParseLine, "nextParseLine", 0, "Next parse line number")
	transformCmd.PersistentFlags().IntVar(&qps, "qps", 1, "Queries per second limit")
	transformCmd.PersistentFlags().IntVar(&parallel, "parallel", 0, "Number of parallel workers")
	transformCmd.PersistentFlags().Int64Var(&limit, "limit", 0, "Account Limit")
	transformCmd.PersistentFlags().Int64Var(&timeout, "timeout", 0, "Timeout in seconds")
	transformCmd.PersistentFlags().StringVar(&inputExample, "inputExample", "matt.carpenter1411@gmail.com:Carpie14", "Input example like matt.carpenter1411@gmail.com:Carpie14")
	transformCmd.PersistentFlags().StringVar(&outputExample, "outputExample", "matt.carpenter1411@gmail.com:Carpie14 | ATK = <accesstoken>", "Output example like matt.carpenter1411@gmail.com:Carpie14 | ATK = <accesstoken>")
	transformCmd.PersistentFlags().BoolVar(&onlyATK, "onlyATK", false, "Output only the ATK if set to true")
	transformCmd.PersistentFlags().StringVar(&endpoint, "endpoint", "https://replace-your-url/api/auth", "provider your endpoint")
	transformCmd.PersistentFlags().StringVar(&delimiterInput, "delimiters", ":,----", "Comma-separated list of custom delimiters for parsing")
	delimiters = strings.Split(delimiterInput, ",") // 使用逗号分隔用户输入的字符串
	matchCmd.Flags().StringVarP(&outputFile, "output", "o", "", "Path to the output file (optional)")

	passAccessTokenDelimiter = parseDelimiter(inputExample, outputExample)
}

var rootCmd = &cobra.Command{}
var transformCmd = &cobra.Command{
	Use:   "transform [input file] [output file]",
	Short: "Transform appends a specified string to each line of the input file",
	Args:  cobra.RangeArgs(1, 2),
	Run:   transform,
}

var matchCmd = &cobra.Command{
	Use:   "match [email file] [database file]",
	Short: "Match emails with passwords",
	Args:  cobra.ExactArgs(2),
	Run:   matchEmails,
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

func matchEmails(cmd *cobra.Command, args []string) {
	emailFile = args[0]
	dbFile = args[1]
	emails, err := readLines(emailFile)
	if err != nil {
		slog.Error(fmt.Sprintf("Error reading email file: %v", err))
		return
	}

	emailMap := make(map[string]bool)
	for _, email := range emails {
		emailMap[email] = true
	}

	var outputWriter *bufio.Writer
	if outputFile != "" {
		file, err := os.Create(outputFile)
		if err != nil {
			slog.Error(fmt.Sprintf("Error creating output file: %v", err))
			return
		}
		defer file.Close()
		outputWriter = bufio.NewWriter(file)
	}

	dbFileHandle, err := os.Open(dbFile)
	if err != nil {
		slog.Error(fmt.Sprintf("Error opening database file: %v", err))
		return
	}
	defer dbFileHandle.Close()

	scanner := bufio.NewScanner(dbFileHandle)
	for scanner.Scan() {
		line := scanner.Text()
		delimiter := findDelimiter(line)

		parts := strings.Split(line, delimiter)
		if len(parts) >= 2 && emailMap[parts[0]] {
			username := parts[0]
			password := strings.Join(parts[1:], delimiter)
			output := username + delimiter + password
			if outputWriter != nil {
				outputWriter.WriteString(output + "\n")
			} else {
				fmt.Println(output)
			}
		}
	}

	if outputWriter != nil {
		outputWriter.Flush()
	}

	if err := scanner.Err(); err != nil {
		slog.Error(fmt.Sprintf("Error reading database file: %v", err))
	}
}

func readLines(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
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
	skipUsername, breakpointResumption, err := calculateSkipLine(outputFile)
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

	entries, nextParseLine, err := processInputFile(inputFile, outputFile, skipUsername, breakpointResumption)
	if err != nil {
		outputFile.Close()
		return nil, 0, nil, err
	}

	return entries, nextParseLine, outputFile, nil
}
func findLastNonEmptyLine(file *os.File) (string, error) {
	const bufSize = 1024
	buf := make([]byte, bufSize)
	var lastLine string
	stat, err := file.Stat()
	if err != nil {
		return "", err
	}
	fileSize := stat.Size()

	// Start from the end of the file
	for offset := fileSize; offset > 0; {
		readSize := bufSize
		if offset < int64(bufSize) {
			readSize = int(offset)
		}

		offset -= int64(readSize)
		_, err := file.Seek(offset, io.SeekStart)
		if err != nil {
			return "", err
		}

		_, err = file.Read(buf[:readSize])
		if err != nil {
			return "", err
		}

		// Process the buffer in reverse order to find the last non-empty line
		for i := readSize - 1; i >= 0; i-- {
			if buf[i] == '\n' {
				if len(lastLine) > 0 {
					// Reverse the collected line (as we read it backwards)
					return reverseString(lastLine), nil
				}
			} else {
				lastLine += string(buf[i])
			}
		}
	}

	// Handle the case where the last line is also the first line (no newlines)
	if len(lastLine) > 0 {
		return reverseString(lastLine), nil
	}

	return "", nil
}

func reverseString(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}
func calculateSkipLine(file *os.File) (username string, breakpointResumption bool, err error) {
	lastLine, err := findLastNonEmptyLine(file)
	if err != nil {
		return "", false, err
	}

	delimiter := findDelimiter(lastLine)
	fields := strings.Fields(lastLine)
	if len(fields) > 0 {
		if strings.Contains(fields[0], "@") && strings.Contains(fields[0], ".") && delimiter != "" {
			split := strings.Split(fields[0], delimiter)
			if len(split) >= 2 {
				username = split[0]
			}
		}

		if username == "" {
			var accessToken string
			n, err := fmt.Sscanf(lastLine, "ATK = %s", &accessToken)
			if err == nil && n == 1 {
				username, _ = checkInfo(accessToken)
			}
		}
	}

	if username == "" {
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

	return username, username != "", nil
}

var userPassDelimiterMap = map[string]struct{}{}

var userPassDelimiterReMap = map[string]*regexp.Regexp{}
var oldnew []string

func processInputFile(inputFile *os.File, outputFile *os.File, skipUsername string, breakpointResumption bool) ([]entry, int, error) {
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

		var (
			username string
			password string
		)

		delimiter := findDelimiter(line)
		if breakpointResumption {
			if strings.Contains(line, "@") && strings.Contains(line, ".") && delimiter != "" {
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

				split := strings.Split(line, userPassDelimiter)
				if len(split) >= 2 {
					username = split[0]
					password = strings.Join(split[1:], userPassDelimiter)
					if nextParseLine == 0 {
						if skipUsername == username {
							nextParseLine = lineCount + 1
						}
						username = ""
						password = ""

					}

				} else {
					slog.Error(fmt.Sprintf("skip %d, invalid line: %s, breakpointResumption: %t", lineCount, line, breakpointResumption))
				}
			} else {
				slog.Error(fmt.Sprintf("skip %d, invalid line: %s, breakpointResumption: %t", lineCount, line, breakpointResumption))
			}

		} else {
			if strings.Contains(line, "@") && strings.Contains(line, ".") {
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
				split := strings.Split(line, userPassDelimiter)
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

func main() {
	rootCmd.AddCommand(matchCmd)
	rootCmd.AddCommand(transformCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
