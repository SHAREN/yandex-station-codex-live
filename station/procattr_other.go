//go:build !linux

package main

import "os/exec"

func configurePlaybackProcess(cmd *exec.Cmd) {}
func restartMediadAfterForcedPlaybackKill()  {}
