package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	maxOutputSize = 512 * 1024 // 512KB
	maxSourceSize = 100 * 1024 // 100KB
	maxExecTime   = 10 * time.Second
	workspaceDir  = "/tmp/codhoot-workspace"
	srcFilename   = "main.py"
)

type CompileRequest struct {
	Source string `json:"source"`
	// Files plus EntryFile enable the multi-file contract the backend uses.
	// Source is still accepted so older callers keep working.
	Files     []File `json:"files,omitempty"`
	EntryFile string `json:"entry_file,omitempty"`
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

// maxConcurrentJobs bounds concurrent compiler+runner processes so a burst of
// students cannot OOM the 512MB free-tier container (which would restart it and
// wipe every warm cache). Requests queue up to their own deadline instead.
//
// The default is this language's COMPILER_CONCURRENCY_* on the backend, so the
// service remains the ceiling even if that configuration drifts upward: the box
// is 0.1 CPU with 512 MB, and each in-flight toolchain costs both a timeslice
// and real memory. MAX_CONCURRENT_JOBS overrides it per deployment.
var maxConcurrentJobs = concurrentJobsFromEnv(4)

// concurrentJobsFromEnv reads MAX_CONCURRENT_JOBS, clamped to 1..8 so a typo
// cannot silently remove the ceiling.
func concurrentJobsFromEnv(def int) int {
	n, err := strconv.Atoi(os.Getenv("MAX_CONCURRENT_JOBS"))
	if err != nil || n < 1 {
		return def
	}
	if n > 8 {
		return 8
	}
	return n
}

var jobSem = make(chan struct{}, maxConcurrentJobs)

// uids hands each in-flight job its own sandbox uid so a job's working directory
// is genuinely private to it. See harden.go for why a single shared uid is not
// isolation.
var uids = newUIDPool()

func main() {
	// Sandboxed path: when re-invoked as the trampoline, apply rlimits and exec
	// the real compiler or runtime. Returns only if the trampoline fails.
	if sandboxExec() {
		return
	}

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
	// Only POST /compile is authenticated. /health and /health/live must stay
	// open or Render marks the service unhealthy and restarts it in a loop.
	mux.Handle("POST /compile", VerifyHMAC(http.HandlerFunc(handleCompile)))
	mux.HandleFunc("GET /health/live", handleHealth)
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("GET /", handleIndex)

	handler := corsMiddleware(loggingMiddleware(mux))

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      handler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: maxExecTime + 20*time.Second,
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
//
// Every child is confined to uid's private job directory, run with a fixed
// minimal environment, capped by rlimits, and its output is captured through a
// bounded writer so a runaway program cannot exhaust the service's memory.
func runCommand(parent context.Context, timeout time.Duration, dir string, uid uint32, name string, args ...string) ([]byte, int, bool) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	self, err := selfPath()
	if err != nil {
		return nil, -1, false
	}

	// Run the sandbox trampoline (this same binary), which applies rlimits and
	// then execs the real target. That is how rlimits get applied at all, since
	// Go's os/exec cannot set them in a child.
	full := append([]string{sandboxExecFlag, name}, args...)

	cmd := exec.CommandContext(ctx, self, full...)
	cmd.Dir = dir
	cmd.Env = childEnv(dir)
	cmd.SysProcAttr = hardenProcAttr(uid)

	killGroup := func() {
		for i := 0; i < 200 && (cmd.Process == nil || cmd.Process.Pid <= 0); i++ {
			time.Sleep(10 * time.Millisecond)
		}
		if cmd.Process != nil && cmd.Process.Pid > 0 {
			killProcessGroup(cmd.Process.Pid)
		}
	}
	go func() {
		<-ctx.Done()
		killGroup()
	}()

	cw := &cappedWriter{max: maxOutputSize}
	cmd.Stdout = cw
	cmd.Stderr = cw
	runErr := cmd.Run()

	out := cw.Bytes()
	if ctx.Err() == context.DeadlineExceeded {
		return out, -1, true
	}
	if runErr != nil {
		if ee, ok := runErr.(*exec.ExitError); ok {
			return out, ee.ExitCode(), false
		}
		return out, 1, false
	}
	return out, 0, false
}

func handleCompile(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	var req CompileRequest
	limitBody(w, r)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body", 0, 0)
		return
	}

	// A multi-file request carries no source field; resolveEntry validates the
	// file set instead, so these single-source checks apply only when it is absent.
	if len(req.Files) == 0 {
		if strings.TrimSpace(req.Source) == "" {
			writeError(w, http.StatusBadRequest, "Source code is required", 0, 0)
			return
		}
		if len(req.Source) > maxSourceSize {
			writeError(w, http.StatusBadRequest, "Source code exceeds maximum size", 0, 0)
			return
		}
	}

	select {
	case jobSem <- struct{}{}:
		defer func() { <-jobSem }()
	case <-r.Context().Done():
		writeError(w, http.StatusServiceUnavailable, "Compiler is busy, try again", 0, 0)
		return
	}

	// Each job gets its own uid and a private random 0700 directory. Sharing one
	// predictable directory let concurrent jobs read each other's source and
	// rewrite it before it was executed.
	uid := uids.acquire()
	defer uids.release(uid)

	jobDir, err := newJobDir(uid)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to create workspace", 0, 0)
		return
	}
	defer os.RemoveAll(jobDir)

	ep, err := resolveEntry(req.Source, req.Files, req.EntryFile, srcFilename)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), 0, 0)
		return
	}
	srcFile, werr := writeEntry(jobDir, ep, uid)
	if werr != nil {
		writeError(w, http.StatusInternalServerError, "Failed to write source", 0, 0)
		return
	}
	// The file was created by the service (root); give it to the sandbox uid so
	// the unprivileged interpreter can read it and nothing else can.
	if err := os.Chown(srcFile, int(uid), int(uid)); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to write source", 0, 0)
		return
	}

	execStart := time.Now()
	output, exitCode, timedOut := runCommand(context.Background(), maxExecTime, jobDir, uid, "python3", srcFile)
	execMs := time.Since(execStart).Milliseconds()

	truncated := false
	if len(output) >= maxOutputSize {
		output = output[:maxOutputSize]
		truncated = true
	}

	// Strip terminal control sequences and internal paths before the client sees it.
	clean := sanitizeOutput(string(output), jobDir)
	resp := CompileResponse{
		Success:         exitCode == 0,
		Output:          clean,
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
