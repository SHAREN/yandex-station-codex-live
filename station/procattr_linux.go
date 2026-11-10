//go:build linux

package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

func configurePlaybackProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}
}

func restartMediadAfterForcedPlaybackKill() {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		log.Printf("mediad recovery scan failed: %v", err)
		return
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		cmdline, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
		if err != nil {
			continue
		}
		cmd := strings.TrimSpace(strings.ReplaceAll(string(cmdline), "\x00", " "))
		if cmd != "/system/vendor/quasar/maind --mediad" {
			continue
		}
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
			log.Printf("mediad recovery kill failed pid=%d error=%v", pid, err)
			return
		}
		log.Printf("mediad recovery requested after forced playback kill pid=%d", pid)
		return
	}
	log.Printf("mediad recovery skipped: process not found")
}
