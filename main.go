package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// 日志数据结构
type LogData struct {
	ApplicationID string `json:"application_id"`
	LogLevel      string `json:"log_level"`
	Timestamp     string `json:"timestamp"`
	LogMessage    string `json:"log_message"`
}

// 上传日志到 /upload 接口
func uploadLog(logData LogData, uploadURL string) error {
	jsonData, err := json.Marshal(logData)
	if err != nil {
		return fmt.Errorf("failed to marshal log data: %v", err)
	}

	// 发送 POST 请求到 /upload
	resp, err := http.Post(uploadURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to upload log: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("failed to upload log: %s", string(body))
	}

	return nil
}

// 解析日志行并上传
func processLogFile(filePath, appID, uploadURL string, wg *sync.WaitGroup) {
	defer wg.Done()

	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("failed to open file %s: %v", filePath, err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		// 解析日志行（假设日志格式为：[timestamp] [loglevel]: log message）
		parts := strings.SplitN(line, ": ", 2)
		if len(parts) != 2 {
			log.Printf("invalid log format in file %s: %s", filePath, line)
			continue
		}

		metaParts := strings.SplitN(parts[0], "] [", 2)
		if len(metaParts) != 2 {
			log.Printf("invalid log format in file %s: %s", filePath, line)
			continue
		}

		timestamp := strings.Trim(metaParts[0], "[]")
		logLevel := strings.Trim(metaParts[1], "[]")
		logMessage := parts[1]

		// 构造日志数据
		logData := LogData{
			ApplicationID: appID,
			LogLevel:      logLevel,
			Timestamp:     timestamp,
			LogMessage:    logMessage,
		}

		// 上传日志
		if err := uploadLog(logData, uploadURL); err != nil {
			log.Printf("failed to upload log from file %s: %v", filePath, err)
		} else {
			log.Printf("successfully uploaded log from file %s: %s", filePath, logMessage)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("error reading file %s: %v", filePath, err)
	}
}

// 遍历目录并上传日志
func uploadLogsFromDir(dirPath, appID, uploadURL string) {
	var wg sync.WaitGroup

	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		log.Fatalf("failed to read directory %s: %v", dirPath, err)
	}

	for _, file := range files {
		if !file.IsDir() {
			wg.Add(1)
			filePath := filepath.Join(dirPath, file.Name())
			go processLogFile(filePath, appID, uploadURL, &wg)
		}
	}

	wg.Wait()
}

func main() {
	// 配置
	dirPath := "test"                           // 日志文件目录
	appID := "seata"                            // 应用程序ID
	uploadURL := "http://localhost:8080/upload" // 日志上传URL

	start := time.Now()
	uploadLogsFromDir(dirPath, appID, uploadURL)
	fmt.Printf("Finished uploading logs in %v\n", time.Since(start))
}
