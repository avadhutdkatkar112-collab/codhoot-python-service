package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	maxOutputSize     = 512 * 1024 // 512KB
	maxSourceSize     = 100 * 1024 // 100KB
	maxExecTime       = 10 * time.Second
	maxConcurrentJobs = 4
	workspaceDir      = "/tmp/codhoot-workspace"
	srcFilename       = "main.py"
)

type CompileRequest struct {
	Source string `json:"source"`
}

type CompileResponse struct {
	Success         bool   `json:"success"`
	Output          string `json:"output,omitempty"`
	Error           string `json:"error,omitempty"`
	ExitCode        int    `json:"exit_code"`
	CompileTime     int64  `json:"compile_time_ms"`
	ExecuteTime     int64  `json:"execute_time_ms"`
	Timeout         bool   `json:"timeout,omitempty"`
	OutputTruncated bool   `json:"output_truncated,omitempty"`
}

type HealthResponse struct {
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

var jobSem = make(chan struct{}, maxConcurrentJobs)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}

	os.MkdirAll(workspaceDir, 0755)
	defer os.RemoveAll(workspaceDir)

	if _, err := exec.LookPath("python3"); err != nil {
		log.Fatalf("python3 not found: %v", err)
	}
	log.Println("Python 3 found, ready to execute")

	mux := http.NewServeMux()
	mux.HandleFunc("POST /compile", handleCompile)
	mux.HandleFunc("GET /health/live", handleHealth)
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("GET /", handleIndex)

	handler := corsMiddleware(loggingMiddleware(mux))

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      handler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: maxExecTime + 10*time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdownCtx)
	}()

	log.Printf("Python Compiler Service listening on :%s", port)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(HealthResponse{
		Status:    "healthy",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	})
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(map[string]string{
		"service": "codhoot-python-compiler",
		"version": "2.0.0",
		"usage":   "POST /compile with {\"source\": \"...\"}",
	})
}

// runCommand runs a command, killing the whole process group on timeout so
// orphaned children never accumulate on the 512MB container.
func runCommand(parent context.Context, timeout time.Duration, dir string, name string, args ...string) ([]byte, int, bool) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	killGroup := func() {
		for i := 0; i < 200 && (cmd.Process == nil || cmd.Process.Pid <= 0); i++ {
			time.Sleep(10 * time.Millisecond)
		}
		if cmd.Process != nil && cmd.Process.Pid > 0 {
			syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
	}
	go func() {
		<-ctx.Done()
		killGroup()
	}()

	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return out, -1, true
	}
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return out, ee.ExitCode(), false
		}
		return out, 1, false
	}
	return out, 0, false
}

func handleCompile(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	var req CompileRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body", 0, 0)
		return
	}

	if strings.TrimSpace(req.Source) == "" {
		writeError(w, http.StatusBadRequest, "Source code is required", 0, 0)
		return
	}

	if len(req.Source) > maxSourceSize {
		writeError(w, http.StatusBadRequest, "Source code exceeds maximum size", 0, 0)
		return
	}

	select {
	case jobSem <- struct{}{}:
		defer func() { <-jobSem }()
	case <-r.Context().Done():
		writeError(w, http.StatusServiceUnavailable, "Compiler is busy, try again", 0, 0)
		return
	}

	jobID := fmt.Sprintf("%d", time.Now().UnixNano())
	jobDir := filepath.Join(workspaceDir, jobID)
	os.MkdirAll(jobDir, 0755)
	defer os.RemoveAll(jobDir)

	srcFile := filepath.Join(jobDir, srcFilename)
	if err := os.WriteFile(srcFile, []byte(req.Source), 0644); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to write source", 0, 0)
		return
	}

	execStart := time.Now()
	output, exitCode, timedOut := runCommand(context.Background(), maxExecTime, jobDir, "python3", srcFile)
	execMs := time.Since(execStart).Milliseconds()

	truncated := false
	if len(output) > maxOutputSize {
		output = output[:maxOutputSize]
		truncated = true
	}

	resp := CompileResponse{
		Success:         exitCode == 0,
		Output:          string(output),
		ExitCode:        exitCode,
		CompileTime:     0,
		ExecuteTime:     execMs,
		Timeout:         timedOut,
		OutputTruncated: truncated,
	}
	if timedOut {
		resp.Error = "Execution timed out (limit: 10s)"
	} else if exitCode != 0 && resp.Output == "" {
		resp.Error = "Execution failed"
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)

	log.Printf("Compile: exit=%d exec=%dms total=%dms timeout=%v",
		exitCode, execMs, time.Since(start).Milliseconds(), timedOut)
}

func writeError(w http.ResponseWriter, status int, message string, compileMs, execMs int64) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(CompileResponse{
		Success:     false,
		Error:       message,
		ExitCode:    -1,
		CompileTime: compileMs,
		ExecuteTime: execMs,
	})
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}