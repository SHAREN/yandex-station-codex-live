package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/thesyncim/gopus"
	"github.com/thesyncim/gopus/container/ogg"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	bridgeBaseURL     = "http://127.0.0.1:8443"
	audioAddr         = "127.0.0.1:9850"
	wakeNotifyAddr    = "127.0.0.1:18769"
	nativeVADLog      = "/bkupgrade/data/quasar/daemons_logs/maind.log"
	quasarDir         = "/system/vendor/quasar"
	quasarMain        = "/system/vendor/quasar/maind"
	memberAudioData   = 158
	memberAudioReq    = 159
	maxIPCFrame       = 1 << 20
	pollInterval      = 500 * time.Millisecond
	reconnectBackoff  = time.Second
	standbyRearmDelay = 150 * time.Millisecond
)

var (
	modeURL = envOrDefault("STATION_CODEX_MODE_URL", "http://homeassistant.local:7872/api/station/lite/codex-mode")
	liveURL = envOrDefault("STATION_CODEX_WS_URL", "ws://homeassistant.local:7861/api/station/lite/live")
)

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

type modeHealth struct {
	Mode string `json:"mode"`
}

type assistantModeCache struct {
	mu      sync.RWMutex
	mode    string
	updated time.Time
}

const assistantModeCacheTTL = 3 * time.Second

func newAssistantModeCache() *assistantModeCache {
	return &assistantModeCache{mode: "alice"}
}

func (c *assistantModeCache) store(mode string, now time.Time) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.mode = mode
	c.updated = now
	c.mu.Unlock()
}

func (c *assistantModeCache) load(now time.Time) string {
	if c == nil {
		return "alice"
	}
	c.mu.RLock()
	mode, updated := c.mode, c.updated
	c.mu.RUnlock()
	if updated.IsZero() || now.Sub(updated) > assistantModeCacheTTL {
		return "alice"
	}
	return mode
}

type playback struct {
	mu          sync.Mutex
	mode        string
	pipeW       *io.PipeWriter
	cmd         *exec.Cmd
	stdin       io.WriteCloser
	decodeDone  chan error
	processDone chan error
	stockDone   chan struct{}
	pending     []byte
	oggScan     []byte
	awaitBOS    bool
	idleTimer   *time.Timer
}

const flightCaptureLimit = 12 << 20

// flightRecorder keeps one current and one previous raw Station session in tmpfs.
// It is deliberately bounded and never writes audio to persistent /data storage.
type flightRecorder struct {
	mu         sync.Mutex
	started    time.Time
	events     *os.File
	mic        *os.File
	downlink   *os.File
	micBytes   int64
	downBytes  int64
	micCapped  bool
	downCapped bool
}

func newFlightRecorder() *flightRecorder {
	base := "/tmp/yandex-lite-flight"
	_ = os.MkdirAll(base, 0755)
	_ = os.RemoveAll(base + "/previous")
	_ = os.Rename(base+"/current", base+"/previous")
	current := base + "/current"
	if err := os.MkdirAll(current, 0755); err != nil {
		log.Printf("flight recorder mkdir failed: %v", err)
		return nil
	}
	open := func(name string) *os.File {
		f, err := os.OpenFile(current+"/"+name, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("flight recorder open %s failed: %v", name, err)
			return nil
		}
		return f
	}
	r := &flightRecorder{started: time.Now(), events: open("events.jsonl"), mic: open("vqe0.s16le"), downlink: open("codex-downlink.bin")}
	r.event("session_start", map[string]any{"mic_format": "s16le/16000/mono", "downlink": "websocket-binary"})
	return r
}

func (r *flightRecorder) close() {
	if r == nil {
		return
	}
	r.event("session_close", map[string]any{"mic_bytes": r.micBytes, "downlink_bytes": r.downBytes})
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, f := range []*os.File{r.events, r.mic, r.downlink} {
		if f != nil {
			_ = f.Sync()
			_ = f.Close()
		}
	}
}

func (r *flightRecorder) event(kind string, fields map[string]any) {
	if r == nil || r.events == nil {
		return
	}
	row := map[string]any{
		"ts":     time.Now().UTC().Format(time.RFC3339Nano),
		"age_ms": time.Since(r.started).Milliseconds(),
		"event":  kind,
		"fields": fields,
	}
	b, err := json.Marshal(row)
	if err != nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, _ = r.events.Write(append(b, '\n'))
}

func (r *flightRecorder) writeMic(p []byte) {
	if r == nil || r.mic == nil || len(p) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.micBytes >= flightCaptureLimit {
		if !r.micCapped {
			r.micCapped = true
			log.Printf("flight recorder microphone cap reached bytes=%d", r.micBytes)
		}
		return
	}
	remaining := int64(flightCaptureLimit) - r.micBytes
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, _ := r.mic.Write(p)
	r.micBytes += int64(n)
}

func (r *flightRecorder) writeDownlink(p []byte) {
	if r == nil || r.downlink == nil || len(p) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.downBytes >= flightCaptureLimit {
		if !r.downCapped {
			r.downCapped = true
			log.Printf("flight recorder downlink cap reached bytes=%d", r.downBytes)
		}
		return
	}
	remaining := int64(flightCaptureLimit) - r.downBytes
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, _ := r.downlink.Write(p)
	r.downBytes += int64(n)
}

type sessionStats struct {
	mu                     sync.Mutex
	started                time.Time
	micChunks              uint64
	micBytes               uint64
	micPeak                int
	micChunksWhilePlayback uint64
	geminiRxChunks         uint64
	geminiRxBytes          uint64
	interrupts             uint64
	ends                   uint64
	firstMicLogged         bool
	firstGeminiLogged      bool
}

func newSessionStats() *sessionStats { return &sessionStats{started: time.Now()} }

func pcmPeakS16LE(b []byte) int {
	peak := 0
	for i := 0; i+1 < len(b); i += 2 {
		v := int(int16(binary.LittleEndian.Uint16(b[i : i+2])))
		if v < 0 {
			v = -v
		}
		if v > peak {
			peak = v
		}
	}
	return peak
}

func (s *sessionStats) addMic(pcm []byte, duringPlayback bool) {
	peak := pcmPeakS16LE(pcm)
	s.mu.Lock()
	s.micChunks++
	s.micBytes += uint64(len(pcm))
	if peak > s.micPeak {
		s.micPeak = peak
	}
	if duringPlayback {
		s.micChunksWhilePlayback++
	}
	first := !s.firstMicLogged
	if first {
		s.firstMicLogged = true
	}
	age := time.Since(s.started)
	s.mu.Unlock()
	if first {
		log.Printf("first_vqe_pcm after_ms=%d bytes=%d peak=%d", age.Milliseconds(), len(pcm), peak)
	}
}

func (s *sessionStats) addGemini(data []byte) {
	s.mu.Lock()
	s.geminiRxChunks++
	s.geminiRxBytes += uint64(len(data))
	first := !s.firstGeminiLogged
	if first {
		s.firstGeminiLogged = true
	}
	age := time.Since(s.started)
	s.mu.Unlock()
	if first {
		log.Printf("first_gemini_pcm after_ms=%d bytes=%d", age.Milliseconds(), len(data))
	}
}

func (s *sessionStats) addControl(kind string) {
	s.mu.Lock()
	if kind == "interrupt" {
		s.interrupts++
	} else if kind == "end" {
		s.ends++
	}
	s.mu.Unlock()
}

func (s *sessionStats) logLoop(ctx context.Context, p *playback) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.mu.Lock()
			age := time.Since(s.started).Milliseconds()
			micChunks, micBytes, micPeak := s.micChunks, s.micBytes, s.micPeak
			micDuring := s.micChunksWhilePlayback
			rxChunks, rxBytes := s.geminiRxChunks, s.geminiRxBytes
			interrupts, ends := s.interrupts, s.ends
			s.mu.Unlock()
			log.Printf("live_stats age_ms=%d mic_chunks=%d mic_bytes=%d mic_peak=%d mic_chunks_while_playback=%d gemini_rx_chunks=%d gemini_rx_bytes=%d playback_active=%t interrupts=%d ends=%d", age, micChunks, micBytes, micPeak, micDuring, rxChunks, rxBytes, p.active(), interrupts, ends)
		}
	}
}

const playbackPrebufferBytes = 1536 // ~150-200 ms at typical Opus rates: enough to smooth mediad startup without hiding short replies.

func bridgeTakeoverActive() bool {
	client := &http.Client{Timeout: 350 * time.Millisecond}
	resp, err := client.Get(bridgeBaseURL + "/__gemini/status")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	var state struct {
		Active bool `json:"active"`
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&state) == nil && state.Active
}

func (p *playback) startStockLocked() error {
	if p.pipeW != nil {
		return nil
	}
	pr, pw := io.Pipe()
	req, err := http.NewRequest(http.MethodPost, bridgeBaseURL+"/__gemini/tts/stream", pr)
	if err != nil {
		_ = pr.Close()
		_ = pw.Close()
		return err
	}
	req.Header.Set("Content-Type", "audio/opus")
	done := make(chan struct{})
	p.mode = "stock_tts"
	p.pipeW = pw
	p.stockDone = done
	go func() {
		defer close(done)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Printf("stock TTS stream request failed: %v", err)
			return
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		if resp.StatusCode != http.StatusNoContent {
			log.Printf("stock TTS stream ended with status=%d", resp.StatusCode)
		} else {
			log.Printf("stock TTS stream ended cleanly")
		}
	}()
	log.Printf("playback backend=stock_tts format=audio/opus container=ogg")
	return nil
}

func decodeOggOpusToPCM(r io.Reader, w io.Writer) error {
	stream, err := ogg.NewReader(r)
	if err != nil {
		return fmt.Errorf("ogg reader: %w", err)
	}
	channels := int(stream.Channels())
	if channels != 1 {
		return fmt.Errorf("unsupported Opus channels=%d", channels)
	}
	cfg := gopus.DefaultDecoderConfig(48000, channels)
	cfg.MaxPacketBytes = 61_440
	decoder, err := gopus.NewDecoder(cfg)
	if err != nil {
		return fmt.Errorf("opus decoder: %w", err)
	}
	pcm := make([]int16, cfg.MaxPacketSamples*channels)
	raw := make([]byte, len(pcm)*2)
	preSkip := int(stream.PreSkip())
	for {
		packet, _, err := stream.ReadPacket()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("ogg packet: %w", err)
		}
		n, err := decoder.DecodeInt16(packet, pcm)
		if err != nil {
			return fmt.Errorf("opus decode: %w", err)
		}
		start := 0
		if preSkip > 0 {
			drop := min(preSkip, n)
			preSkip -= drop
			start = drop
		}
		count := (n - start) * channels
		if count <= 0 {
			continue
		}
		for i := 0; i < count; i++ {
			binary.LittleEndian.PutUint16(raw[i*2:], uint16(pcm[start*channels+i]))
		}
		if _, err := w.Write(raw[:count*2]); err != nil {
			return err
		}
	}
}

func logALSADiagnostics(stage string) {
	status := "unavailable"
	if data, err := os.ReadFile("/proc/asound/card0/pcm0p/sub0/status"); err == nil {
		status = strings.TrimSpace(string(data))
	}
	owners := "none"
	if out, err := exec.Command("/usr/bin/fuser", "/dev/snd/pcmC0D0p").CombinedOutput(); len(out) > 0 {
		owners = strings.TrimSpace(string(out))
	} else if err != nil {
		owners = "none"
	}
	log.Printf("alsa_diag stage=%s pcmC0D0p_status=%q owners=%q", stage, status, owners)
	for _, field := range strings.Fields(owners) {
		pid := strings.Trim(field, "0123456789")
		if pid != "" || field == "" {
			continue
		}
		cmdline, err := os.ReadFile("/proc/" + field + "/cmdline")
		if err != nil {
			continue
		}
		text := strings.TrimSpace(strings.ReplaceAll(string(cmdline), "\x00", " "))
		if text != "" {
			log.Printf("alsa_diag_owner stage=%s pid=%s cmd=%q", stage, field, text)
		}
	}
}

func (p *playback) startALSALocked() error {
	if p.cmd != nil && p.cmd.Process != nil {
		return nil
	}
	pr, pw := io.Pipe()
	cmd := exec.Command(
		"/usr/bin/aplay", "-q", "-t", "raw", "-f", "S16_LE",
		"-r", "48000", "-c", "1", "-D", "default",
		"--buffer-time=180000", "--period-time=20000",
	)
	configurePlaybackProcess(cmd)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		_ = pr.Close()
		_ = pw.Close()
		return err
	}
	cmd.Stdout = io.Discard
	logALSADiagnostics("before_aplay_start")
	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = pr.Close()
		_ = pw.Close()
		_ = stdin.Close()
		return fmt.Errorf("aplay stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		_ = pr.Close()
		_ = pw.Close()
		_ = stdin.Close()
		return fmt.Errorf("aplay start: %w", err)
	}
	decodeDone := make(chan error, 1)
	processDone := make(chan error, 1)
	stderrDone := make(chan struct{})
	p.mode, p.pipeW, p.cmd, p.stdin = "alsa_opus", pw, cmd, stdin
	p.decodeDone, p.processDone = decodeDone, processDone
	go func() {
		defer close(stderrDone)
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line != "" {
				log.Printf("aplay_stderr %s", line)
			}
		}
		if err := scanner.Err(); err != nil {
			log.Printf("aplay_stderr_read_error error=%v", err)
		}
	}()
	go func() {
		err := decodeOggOpusToPCM(pr, stdin)
		_ = pr.Close()
		_ = stdin.Close()
		decodeDone <- err
		close(decodeDone)
	}()
	go func() {
		err := cmd.Wait()
		<-stderrDone
		if err != nil {
			logALSADiagnostics("after_aplay_exit")
		}
		processDone <- err
		close(processDone)
	}()
	log.Printf("playback backend=alsa_opus codec=ogg/opus pcm=48000/mono device=default mixer=Master buffer_us=180000 pid=%d", cmd.Process.Pid)
	return nil
}

func stockTTSPlaybackSelected() bool {
	backend := strings.ToLower(strings.TrimSpace(os.Getenv("YANDEX_LITE_PLAYBACK_BACKEND")))
	// Direct ALSA/default is the production path because stock TtsSpeak can
	// report a clean stream while remaining physically silent during local
	// Codex takeover. Keep stock_tts only as an explicit diagnostic override.
	return backend == "stock_tts"
}

func (p *playback) startLocked() error {
	if p.mode != "" {
		return nil
	}
	if stockTTSPlaybackSelected() {
		// Native playback injects audio through the active stock request so the
		// Station keeps mediad/audio-focus/volume/UserStop behavior intact.
		if !bridgeTakeoverActive() {
			return errors.New("stock Gemini takeover is not active")
		}
		return p.startStockLocked()
	}
	return p.startALSALocked()
}

func (p *playback) scheduleIdleLocked() {
	// The voice assistant owns one continuous stock TtsSpeak stream for the whole
	// conversation. A user pause is not a playback boundary: closing here would
	// force the next assistant turn to continue an Ogg stream in a fresh player.
	// Real boundaries are Ogg EOS (when available), interrupt/barge-in, stop,
	// or session end.
	if p.idleTimer != nil {
		p.idleTimer.Stop()
		p.idleTimer = nil
	}
}

func (p *playback) noteOggLocked(b []byte) (bool, error) {
	p.oggScan = append(p.oggScan, b...)
	for {
		if len(p.oggScan) < 27 {
			return false, nil
		}
		if string(p.oggScan[:4]) != "OggS" || p.oggScan[4] != 0 {
			return false, errors.New("invalid Ogg/Opus stream from Gemini")
		}
		segments := int(p.oggScan[26])
		if len(p.oggScan) < 27+segments {
			return false, nil
		}
		pageSize := 27 + segments
		for _, n := range p.oggScan[27 : 27+segments] {
			pageSize += int(n)
		}
		if pageSize > 1<<20 {
			return false, errors.New("oversized Ogg page from Gemini")
		}
		if len(p.oggScan) < pageSize {
			return false, nil
		}
		eos := p.oggScan[5]&0x04 != 0
		p.oggScan = p.oggScan[pageSize:]
		if eos {
			return true, nil
		}
	}
}

func findOggOpusBOS(b []byte) int {
	for off := 0; ; {
		i := bytes.Index(b[off:], []byte("OggS"))
		if i < 0 {
			return -1
		}
		i += off
		if len(b)-i < 27 {
			return -1
		}
		if b[i+4] == 0 && b[i+5]&0x02 != 0 {
			segments := int(b[i+26])
			if len(b)-i < 27+segments {
				return -1
			}
			pageSize := 27 + segments
			for _, n := range b[i+27 : i+27+segments] {
				pageSize += int(n)
			}
			if len(b)-i < pageSize {
				return -1
			}
			if bytes.Contains(b[i:i+pageSize], []byte("OpusHead")) {
				return i
			}
		}
		off = i + 4
		if off >= len(b) {
			return -1
		}
	}
}

func shouldRolloverStockTurn(mode string, payload []byte) (int, bool) {
	if mode != "stock_tts" {
		return -1, false
	}
	idx := findOggOpusBOS(payload)
	return idx, idx >= 0
}

func (p *playback) write(b []byte) error {
	p.mu.Lock()
	payload := b
	if p.awaitBOS {
		p.pending = append(p.pending, b...)
		idx := findOggOpusBOS(p.pending)
		if idx < 0 {
			if len(p.pending) > 64<<10 {
				p.pending = append([]byte(nil), p.pending[len(p.pending)-(8<<10):]...)
			}
			p.mu.Unlock()
			return nil
		}
		payload = append([]byte(nil), p.pending[idx:]...)
		p.pending = nil
		p.awaitBOS = false
		log.Printf("Gemini playback resynchronized on fresh Ogg/Opus BOS")
	}
	if idx, rollover := shouldRolloverStockTurn(p.mode, payload); rollover {
		log.Printf("Gemini new Ogg/Opus BOS while stock TTS is active; rolling to a fresh TtsSpeak")
		p.stopLocked(false)
		payload = append([]byte(nil), payload[idx:]...)
	}
	if p.mode == "" {
		p.pending = append(p.pending, payload...)
		if len(p.pending) < playbackPrebufferBytes {
			p.mu.Unlock()
			return nil
		}
		payload = append([]byte(nil), p.pending...)
		p.pending = nil
		if err := p.startLocked(); err != nil {
			p.mu.Unlock()
			return err
		}
	}
	if p.pipeW == nil {
		p.mu.Unlock()
		return errors.New("Gemini playback stream is unavailable")
	}
	eos, err := p.noteOggLocked(payload)
	if err != nil {
		p.stopLocked(true)
		p.mu.Unlock()
		return err
	}
	pipeW := p.pipeW
	mode := p.mode
	p.mu.Unlock()

	if _, err := pipeW.Write(payload); err != nil {
		p.mu.Lock()
		current := p.pipeW == pipeW
		p.mu.Unlock()
		if !current {
			return nil
		}
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pipeW != pipeW {
		return nil
	}
	if eos {
		log.Printf("Gemini Ogg EOS detected backend=%s; closing reply stream", mode)
		p.stopLocked(false)
		return nil
	}
	p.scheduleIdleLocked()
	return nil
}

func waitPlaybackDone(ch <-chan error, timeout time.Duration) (error, bool) {
	if ch == nil {
		return nil, true
	}
	select {
	case err, ok := <-ch:
		if !ok {
			return nil, true
		}
		return err, true
	case <-time.After(timeout):
		return nil, false
	}
}

func (p *playback) stopLocked(interrupt bool) {
	if p.idleTimer != nil {
		p.idleTimer.Stop()
		p.idleTimer = nil
	}
	p.pending = p.pending[:0]
	p.oggScan = p.oggScan[:0]
	mode := p.mode
	pipeW, stdin, cmd := p.pipeW, p.stdin, p.cmd
	decodeDone, processDone, stockDone := p.decodeDone, p.processDone, p.stockDone
	p.mode, p.pipeW, p.cmd, p.stdin = "", nil, nil, nil
	p.decodeDone, p.processDone, p.stockDone = nil, nil, nil

	if pipeW != nil {
		if interrupt {
			_ = pipeW.CloseWithError(errors.New("interrupted"))
		} else {
			_ = pipeW.Close()
		}
	}

	if mode == "stock_tts" {
		if stockDone != nil {
			select {
			case <-stockDone:
			case <-time.After(750 * time.Millisecond):
				log.Printf("stock TTS close timed out; forcing native stream interrupt")
				req, _ := http.NewRequest(http.MethodPost, bridgeBaseURL+"/__gemini/tts/interrupt", nil)
				if resp, err := (&http.Client{Timeout: 500 * time.Millisecond}).Do(req); err == nil {
					_ = resp.Body.Close()
				}
			}
		}
		return
	}

	if mode == "alsa_opus" {
		// Closing the Ogg pipe lets the decoder close aplay stdin normally. This is
		// important for dmix: SIGKILL can leave the shared PCM in PREPARED with a
		// dead owner, muting both stock Alice and Gemini until mediad is restarted.
		if _, done := waitPlaybackDone(decodeDone, 250*time.Millisecond); !done && stdin != nil {
			_ = stdin.Close()
		}

		if err, done := waitPlaybackDone(processDone, 350*time.Millisecond); done {
			if err != nil && !interrupt {
				log.Printf("ALSA playback process ended with error: %v", err)
			}
			return
		}

		if cmd != nil && cmd.Process != nil {
			_ = cmd.Process.Signal(os.Interrupt)
		}
		if err, done := waitPlaybackDone(processDone, 350*time.Millisecond); done {
			if err != nil && !interrupt {
				log.Printf("ALSA playback process ended after SIGINT: %v", err)
			}
			return
		}

		if cmd != nil && cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
		}
		if err, done := waitPlaybackDone(processDone, 500*time.Millisecond); done {
			if err != nil && !interrupt {
				log.Printf("ALSA playback process ended after SIGTERM: %v", err)
			}
			return
		}

		// Last resort only. Supervisor/PDEATHSIG normally makes this unreachable.
		// Keep a loud diagnostic because a forced kill may require mediad recovery.
		log.Printf("ALSA playback graceful shutdown timed out; forcing kill")
		if cmd != nil && cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_, _ = waitPlaybackDone(processDone, 300*time.Millisecond)
		restartMediadAfterForcedPlaybackKill()
		return
	}

	if stdin != nil {
		_ = stdin.Close()
	}
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_, _ = waitPlaybackDone(processDone, 500*time.Millisecond)
	}
}

func (p *playback) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopLocked(false)
}

func (p *playback) interrupt() {
	p.mu.Lock()
	p.stopLocked(true)
	p.awaitBOS = true
	p.pending = nil
	p.mu.Unlock()
	go func() {
		req, _ := http.NewRequest(http.MethodPost, bridgeBaseURL+"/__gemini/tts/interrupt", nil)
		if resp, err := (&http.Client{Timeout: time.Second}).Do(req); err == nil {
			_ = resp.Body.Close()
		}
	}()
}

func (p *playback) active() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mode != ""
}

func completeInitialStockInput() {
	req, _ := http.NewRequest(http.MethodPost, bridgeBaseURL+"/__gemini/input/complete", nil)
	client := &http.Client{Timeout: 800 * time.Millisecond}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("stock initial VoiceInput complete failed: %v", err)
		return
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		log.Printf("stock initial VoiceInput complete status=%d", resp.StatusCode)
		return
	}
	log.Printf("stock initial VoiceInput completed after Codex connection and first vqe_0 audio")
}

func endStockSession() {
	req, _ := http.NewRequest(http.MethodPost, bridgeBaseURL+"/__gemini/session/end", nil)
	client := &http.Client{Timeout: 1200 * time.Millisecond}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("stock local session end failed: %v", err)
		return
	}
	_ = resp.Body.Close()
	log.Printf("stock local session end status=%d", resp.StatusCode)
}

func normalizeAssistantMode(raw string) (string, bool) {
	mode := strings.ToLower(strings.TrimSpace(raw))
	switch mode {
	case "alice", "stock":
		return "alice", true
	case "quasar", "custom", "gemini":
		// Historical selector value remains assigned to the turn-based brain.
		return "quasar", true
	case "codex", "gpt live", "gpt-live", "gpt-live-1-codex":
		return "codex", true
	default:
		return "alice", false
	}
}

func fetchAssistantMode(ctx context.Context, client *http.Client) (string, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, modeURL, nil)
	if err != nil {
		return "alice", false
	}
	resp, err := client.Do(req)
	if err != nil {
		return "alice", false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "alice", false
	}
	var h modeHealth
	if json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&h) != nil {
		return "alice", false
	}
	return normalizeAssistantMode(h.Mode)
}

func assistantMode(ctx context.Context, client *http.Client) string {
	if mode, ok := fetchAssistantMode(ctx, client); ok {
		return mode
	}
	return "alice"
}

func monitorAssistantMode(ctx context.Context, client *http.Client, cache *assistantModeCache) {
	lastMode := ""
	refresh := func() {
		probeCtx, cancel := context.WithTimeout(ctx, 700*time.Millisecond)
		mode, ok := fetchAssistantMode(probeCtx, client)
		cancel()
		if ok {
			cache.store(mode, time.Now())
			if mode != lastMode {
				log.Printf("assistant_mode_cache mode=%s", mode)
				lastMode = mode
			}
		}
	}
	refresh()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refresh()
		}
	}
}

func codexEnabled(ctx context.Context, client *http.Client) bool {
	return assistantMode(ctx, client) == "codex"
}

func aliceStateStatus(b []byte) (uint64, string, bool) {
	var status uint64
	var text string
	seen := false
	for len(b) > 0 {
		num, typ, l := protowire.ConsumeTag(b)
		if l < 0 {
			return 0, "", false
		}
		b = b[l:]
		switch {
		case num == 1 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return 0, "", false
			}
			status, b, seen = v, b[n:], true
		case num == 2 && typ == protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return 0, "", false
			}
			text, b, seen = string(v), b[n:], true
		default:
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return 0, "", false
			}
			b = b[n:]
		}
	}
	return status, text, seen
}

func cancelStockConversation() error {
	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()
	cmd := exec.CommandContext(ctx, quasarMain, "--cli")
	cmd.Dir = quasarDir
	cmd.Stdin = strings.NewReader("c\nx\n")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	return nil
}

func waitForStockWake(ctx context.Context, modeCache *assistantModeCache) bool {
	addr, err := net.ResolveUDPAddr("udp4", wakeNotifyAddr)
	if err != nil {
		log.Printf("wake notify address invalid: %v", err)
		return false
	}
	conn, err := net.ListenUDP("udp4", addr)
	if err != nil {
		log.Printf("wake notify listener unavailable: %v", err)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(reconnectBackoff):
			return true
		}
	}
	defer conn.Close()
	stopRead := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopRead()

	packet := make([]byte, 64)
	for ctx.Err() == nil {
		n, peer, err := conn.ReadFromUDP(packet)
		if err != nil {
			if ctx.Err() != nil {
				return false
			}
			log.Printf("wake notify listener failed: %v", err)
			return true
		}
		if strings.TrimSpace(string(packet[:n])) != "wake" {
			continue
		}
		decisionStarted := time.Now()
		mode := modeCache.load(decisionStarted)
		decision := "pass"
		switch mode {
		case "codex":
			decision = "takeover"
		case "quasar":
			decision = "quasar"
		}
		if peer != nil {
			_, _ = conn.WriteToUDP([]byte(decision), peer)
		}
		log.Printf("wake_decision decision=%s mode=%s latency_ms=%d", decision, mode, time.Since(decisionStarted).Milliseconds())
		if mode != "codex" {
			// Alice and Quasar Assistant both keep the stock VoiceInput/SpeechKit path.
			continue
		}

		log.Printf("stock Alice VoiceInput detected; Bridge owns the local streaming request")
		// Do not run maind --cli cancel here. Quasar Bridge now answers the stock
		// VoiceInput locally with InputStartAck + streaming AliceResponse and keeps
		// that native dialog alive for the Codex speech session.
		return true
	}
	return false
}

type nativeVADSubscriber struct {
	ch       chan string
	turnOpen bool
}

const nativeYandexVADEndSilence = 650 * time.Millisecond

type nativeVADState struct {
	mu          sync.Mutex
	initialized bool
	rawSpeaking bool
	subscribers map[chan string]*nativeVADSubscriber
	endTimer    *time.Timer
	endSeq      uint64
	endSilence  time.Duration
}

func newNativeVADState() *nativeVADState {
	return &nativeVADState{
		subscribers: make(map[chan string]*nativeVADSubscriber),
		endSilence:  nativeYandexVADEndSilence,
	}
}

func (v *nativeVADState) beginUtterance(source string) {
	v.mu.Lock()
	v.endSeq++
	if v.endTimer != nil {
		v.endTimer.Stop()
		v.endTimer = nil
	}
	started := make([]*nativeVADSubscriber, 0, len(v.subscribers))
	for _, sub := range v.subscribers {
		if !sub.turnOpen {
			sub.turnOpen = true
			started = append(started, sub)
		}
	}
	v.mu.Unlock()
	log.Printf("native_yandex_vad utterance_start source=%s notified=%d", source, len(started))
	for _, sub := range started {
		select {
		case sub.ch <- "speech_start":
		default:
			log.Printf("native_yandex_vad subscriber queue full; dropping event=speech_start")
		}
	}
}

func (v *nativeVADState) raw(speaking bool, source string) {
	v.mu.Lock()
	v.initialized = true
	v.rawSpeaking = speaking
	v.endSeq++
	seq := v.endSeq
	if v.endTimer != nil {
		v.endTimer.Stop()
		v.endTimer = nil
	}

	started := make([]*nativeVADSubscriber, 0, len(v.subscribers))
	openTurn := false
	for _, sub := range v.subscribers {
		if speaking && !sub.turnOpen {
			sub.turnOpen = true
			started = append(started, sub)
		}
		if sub.turnOpen {
			openTurn = true
		}
	}
	if !speaking && openTurn {
		delay := v.endSilence
		v.endTimer = time.AfterFunc(delay, func() {
			v.endAfterSilence(seq, source, delay)
		})
	}
	v.mu.Unlock()

	event := "speech_end_observed"
	if speaking {
		event = "speech_start"
	}
	log.Printf("native_yandex_vad raw_event=%s source=%s notified=%d end_delay_ms=%d", event, source, len(started), v.endSilence.Milliseconds())
	for _, sub := range started {
		select {
		case sub.ch <- "speech_start":
		default:
			log.Printf("native_yandex_vad subscriber queue full; dropping event=speech_start")
		}
	}
}

func (v *nativeVADState) endAfterSilence(seq uint64, source string, delay time.Duration) {
	v.mu.Lock()
	if seq != v.endSeq || v.rawSpeaking {
		v.mu.Unlock()
		return
	}
	v.endTimer = nil
	v.endSeq++
	subscribers := v.closeTurnsLocked()
	v.mu.Unlock()
	v.notifySpeechEnd(subscribers, fmt.Sprintf("%s_silence_%dms", source, delay.Milliseconds()))
}

func (v *nativeVADState) closeTurnsLocked() []*nativeVADSubscriber {
	subscribers := make([]*nativeVADSubscriber, 0, len(v.subscribers))
	for _, sub := range v.subscribers {
		if sub.turnOpen {
			sub.turnOpen = false
			subscribers = append(subscribers, sub)
		}
	}
	return subscribers
}

func (v *nativeVADState) notifySpeechEnd(subscribers []*nativeVADSubscriber, source string) {
	log.Printf("native_yandex_vad endpoint_event=speech_end source=%s notified=%d", source, len(subscribers))
	for _, sub := range subscribers {
		select {
		case sub.ch <- "speech_end":
		default:
			log.Printf("native_yandex_vad subscriber queue full; dropping event=speech_end")
		}
	}
}

func (v *nativeVADState) endUtterance(source string) {
	v.mu.Lock()
	v.initialized = true
	v.rawSpeaking = false
	v.endSeq++
	if v.endTimer != nil {
		v.endTimer.Stop()
		v.endTimer = nil
	}
	subscribers := v.closeTurnsLocked()
	v.mu.Unlock()
	v.notifySpeechEnd(subscribers, source)
}

func (v *nativeVADState) subscribe(ctx context.Context) <-chan string {
	ch := make(chan string, 16)
	v.mu.Lock()
	sub := &nativeVADSubscriber{ch: ch}
	v.subscribers[ch] = sub
	if v.initialized && v.rawSpeaking {
		sub.turnOpen = true
		ch <- "speech_start"
	}
	v.mu.Unlock()
	go func() {
		<-ctx.Done()
		v.mu.Lock()
		delete(v.subscribers, ch)
		v.mu.Unlock()
	}()
	return ch
}

func nativeVADEventFromLine(line string) (speaking bool, ok bool) {
	if !strings.Contains(line, "[vad]") {
		return false, false
	}
	switch {
	case strings.Contains(line, "Speech started"):
		return true, true
	case strings.Contains(line, "Speech finished"):
		return false, true
	default:
		return false, false
	}
}

func latestNativeVADState(data []byte) (speaking bool, ok bool) {
	for _, line := range strings.Split(string(data), "\n") {
		if state, matched := nativeVADEventFromLine(line); matched {
			speaking, ok = state, true
		}
	}
	return speaking, ok
}

func applyNativeVADLine(v *nativeVADState, line string) {
	if speaking, ok := nativeVADEventFromLine(line); ok {
		v.raw(speaking, "maind")
	}
}

// monitorNativeYandexVAD follows the Station firmware's own VAD decisions.
// It deliberately does no acoustic analysis: maind already runs Yandex's VAD
// on the native microphone/VQE path and writes the authoritative boundaries.
// The file transport is isolated here so it can later be replaced by the
// equivalent Quasar capability subscription without changing session logic.
func monitorNativeYandexVAD(ctx context.Context, v *nativeVADState) {
	var f *os.File
	var currentInfo os.FileInfo
	var offset int64
	var pending string

	closeCurrent := func() {
		if f != nil {
			_ = f.Close()
		}
		f = nil
		currentInfo = nil
		offset = 0
		pending = ""
	}
	defer closeCurrent()

	openCurrent := func() bool {
		next, err := os.Open(nativeVADLog)
		if err != nil {
			return false
		}
		info, err := next.Stat()
		if err != nil {
			_ = next.Close()
			return false
		}
		start := info.Size() - 128*1024
		if start < 0 {
			start = 0
		}
		if _, err := next.Seek(start, io.SeekStart); err != nil {
			_ = next.Close()
			return false
		}
		seed, _ := io.ReadAll(next)
		if speaking, ok := latestNativeVADState(seed); ok {
			v.raw(speaking, "maind_seed")
		}
		end, err := next.Seek(0, io.SeekEnd)
		if err != nil {
			_ = next.Close()
			return false
		}
		f, currentInfo, offset = next, info, end
		pending = ""
		log.Printf("native_yandex_vad monitor attached path=%s offset=%d", nativeVADLog, offset)
		return true
	}

	buf := make([]byte, 8192)
	for ctx.Err() == nil {
		if f == nil {
			if !openCurrent() {
				select {
				case <-ctx.Done():
					return
				case <-time.After(250 * time.Millisecond):
					continue
				}
			}
		}

		pathInfo, statErr := os.Stat(nativeVADLog)
		if statErr != nil || currentInfo == nil || !os.SameFile(currentInfo, pathInfo) || pathInfo.Size() < offset {
			closeCurrent()
			continue
		}

		n, err := f.Read(buf)
		if n > 0 {
			offset += int64(n)
			pending += string(buf[:n])
			for {
				idx := strings.IndexByte(pending, '\n')
				if idx < 0 {
					break
				}
				line := pending[:idx]
				pending = pending[idx+1:]
				applyNativeVADLine(v, line)
			}
		}
		if err != nil && !errors.Is(err, io.EOF) {
			log.Printf("native_yandex_vad monitor read failed: %v", err)
			closeCurrent()
			continue
		}
		if n == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(25 * time.Millisecond):
			}
		}
	}
}

func monitorMode(ctx context.Context, client *http.Client, cancel context.CancelFunc) {
	t := time.NewTicker(pollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if !codexEnabled(ctx, client) {
				log.Printf("mode switched away from Codex; closing GPT-Live audio")
				cancel()
				return
			}
		}
	}
}

func monitorActiveStockControl(ctx context.Context, cancel context.CancelFunc, player *playback, nativeVAD *nativeVADState) {
	addr, err := net.ResolveUDPAddr("udp4", wakeNotifyAddr)
	if err != nil {
		log.Printf("active stock-control address invalid: %v", err)
		return
	}
	conn, err := net.ListenUDP("udp4", addr)
	if err != nil {
		log.Printf("active stock-control listener unavailable: %v", err)
		return
	}
	defer conn.Close()
	stopRead := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopRead()
	packet := make([]byte, 32)
	for ctx.Err() == nil {
		n, peer, err := conn.ReadFromUDP(packet)
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("active stock-control listener failed: %v", err)
			}
			return
		}
		switch strings.TrimSpace(string(packet[:n])) {
		case "wake":
			// A repeated stock wake during an active Codex conversation must stay
			// owned by the existing takeover. Previously this listener ignored the
			// packet, so Bridge timed out after 180 ms and failed open to stock Alice.
			// Interrupt only the current reply, acknowledge takeover, and let Bridge
			// replace the native VoiceInput container while the Codex WS/VQE stay alive.
			player.interrupt()
			if peer != nil {
				_, _ = conn.WriteToUDP([]byte("takeover"), peer)
			}
			log.Printf("active stock wake re-taken over by Codex")
		case "speech_end":
			if nativeVAD != nil {
				nativeVAD.endUtterance("alicekit_end_of_utterance")
			}
			log.Printf("stock AliceKit end_of_utterance received")
		case "stop":
			log.Printf("stock UserStop received; interrupting playback and Codex immediately")
			player.interrupt()
			cancel()
			return
		}
	}
}

func writeIPCFrame(w io.Writer, member protowire.Number, body []byte) error {
	p := make([]byte, 4, 16+len(body))
	p = protowire.AppendTag(p, 252, protowire.BytesType)
	p = protowire.AppendBytes(p, body)
	p = protowire.AppendTag(p, 253, protowire.VarintType)
	p = protowire.AppendVarint(p, uint64(member))
	binary.LittleEndian.PutUint32(p, uint32(len(p)-4))
	_, err := w.Write(p)
	return err
}

func readIPCFrame(r io.Reader, buf *[]byte) (protowire.Number, []byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.LittleEndian.Uint32(hdr[:])
	if n == 0 || n > maxIPCFrame {
		return 0, nil, fmt.Errorf("invalid yio_audio frame size %d", n)
	}
	if cap(*buf) < int(n) {
		*buf = make([]byte, n)
	}
	b := (*buf)[:n]
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, nil, err
	}
	var member protowire.Number
	var body []byte
	for len(b) > 0 {
		num, typ, l := protowire.ConsumeTag(b)
		if l < 0 {
			return 0, nil, protowire.ParseError(l)
		}
		b = b[l:]
		switch {
		case num == 252 && typ == protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return 0, nil, protowire.ParseError(n)
			}
			body, b = v, b[n:]
		case num == 253 && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return 0, nil, protowire.ParseError(n)
			}
			member, b = protowire.Number(v), b[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return 0, nil, protowire.ParseError(n)
			}
			b = b[n:]
		}
	}
	return member, body, nil
}

func channelData(b []byte) (name string, data []byte, ok bool) {
	rate, format := uint64(16000), uint64(1)
	for len(b) > 0 {
		num, typ, l := protowire.ConsumeTag(b)
		if l < 0 {
			return "", nil, false
		}
		b = b[l:]
		switch {
		case num == 2 && typ == protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return "", nil, false
			}
			name, b = string(v), b[n:]
		case num == 3 && typ == protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return "", nil, false
			}
			data, b = v, b[n:]
		case (num == 7 || num == 8) && typ == protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return "", nil, false
			}
			if num == 7 {
				rate = v
			} else {
				format = v
			}
			b = b[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return "", nil, false
			}
			b = b[n:]
		}
	}
	return name, data, rate == 16000 && format == 1
}

func streamVQE(ctx context.Context, send func([]byte) error) error {
	d := net.Dialer{Timeout: 2 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", audioAddr)
	if err != nil {
		return err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	// IOChannelRequest.request_vqe (field 3). This is the stock post-VQE/AEC vqe_0 stream.
	req := protowire.AppendTag(nil, 3, protowire.BytesType)
	req = protowire.AppendBytes(req, nil)
	if err := writeIPCFrame(conn, memberAudioReq, req); err != nil {
		return err
	}

	r := bufio.NewReader(conn)
	var buf []byte
	for {
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		member, body, err := readIPCFrame(r, &buf)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		if member != memberAudioData {
			continue
		}
		for b := body; len(b) > 0; {
			num, typ, l := protowire.ConsumeTag(b)
			if l < 0 {
				return protowire.ParseError(l)
			}
			b = b[l:]
			if num != 1 || typ != protowire.BytesType {
				n := protowire.ConsumeFieldValue(num, typ, b)
				if n < 0 {
					return protowire.ParseError(n)
				}
				b = b[n:]
				continue
			}
			ch, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
			name, pcm, ok := channelData(ch)
			if !ok || name != "vqe_0" || len(pcm) == 0 {
				continue
			}
			if err := send(pcm); err != nil {
				return err
			}
		}
	}
}

func runSession(parent context.Context, client *http.Client, nativeVAD *nativeVADState) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	defer endStockSession()
	// Yandex VAD is intentionally diagnostics-only during Codex takeover.
	// The maind monitor keeps logging raw Speech started/finished timestamps,
	// but no Yandex VAD event is forwarded upstream or used to close turns.
	flight := newFlightRecorder()
	defer flight.close()
	go monitorMode(ctx, client, cancel)

	// Capture stock post-VQE/AEC audio immediately after the wake event. The
	// channel keeps a few seconds of fresh PCM while the Codex relay WebSocket is
	// handshaking, so "Алиса, <question>" does not lose the beginning of the
	// question. On overflow, discard the oldest chunk and preserve newest speech.
	audioCh := make(chan []byte, 128)
	var player playback
	go monitorActiveStockControl(ctx, cancel, &player, nativeVAD)
	stats := newSessionStats()
	go stats.logLoop(ctx, &player)
	firstVQEReady := make(chan struct{})
	var firstVQEOnce sync.Once
	captureErr := make(chan error, 1)
	go func() {
		err := streamVQE(ctx, func(pcm []byte) error {
			flight.writeMic(pcm)
			stats.addMic(pcm, player.active())
			firstVQEOnce.Do(func() { close(firstVQEReady) })
			chunk := append([]byte(nil), pcm...)
			select {
			case audioCh <- chunk:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			select {
			case <-audioCh:
			default:
			}
			select {
			case audioCh <- chunk:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		captureErr <- err
		cancel()
	}()

	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second, EnableCompression: false}
	ws, _, err := dialer.DialContext(ctx, liveURL, nil)
	if err != nil {
		return fmt.Errorf("live websocket: %w", err)
	}
	defer ws.Close()
	ws.SetReadLimit(1 << 20)
	log.Printf("Codex GPT-Live full-duplex connected")

	go func() {
		select {
		case <-firstVQEReady:
			// The stock wake/listening request is only a bootstrap envelope. Once
			// post-VQE/AEC audio is flowing and the Codex relay is connected, release that
			// envelope immediately; the independent vqe_0 IPC stream stays alive.
			completeInitialStockInput()
		case <-ctx.Done():
		}
	}()

	defer player.stop()
	readErr := make(chan error, 1)
	go func() {
		for {
			kind, data, err := ws.ReadMessage()
			if err != nil {
				readErr <- err
				cancel()
				return
			}
			switch kind {
			case websocket.BinaryMessage:
				if len(data) > 0 {
					flight.writeDownlink(data)
					stats.addGemini(data)
					if err := player.write(data); err != nil {
						flight.event("playback_write_error", map[string]any{"error": err.Error()})
						readErr <- fmt.Errorf("playback: %w", err)
						cancel()
						return
					}
				}
			case websocket.TextMessage:
				var ctl struct {
					Type string
				}
				if json.Unmarshal(data, &ctl) == nil {
					stats.addControl(ctl.Type)
					flight.event("control", map[string]any{"type": ctl.Type})
					log.Printf("live_control type=%s", ctl.Type)
					switch ctl.Type {
					case "gemini_vad_start":
						// Diagnostic marker only. Gemini itself owns turn start.
					case "gemini_vad_end":
						// Diagnostic marker only. The stock bootstrap VoiceInput is
						// already released as soon as Codex and vqe_0 are both ready.
					case "interrupt":
						player.interrupt()
					case "end":
						cancel()
					}
				}
			}
		}
	}()

	writeErr := make(chan error, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				writeErr <- ctx.Err()
				return
			case pcm := <-audioCh:
				_ = ws.SetWriteDeadline(time.Now().Add(1500 * time.Millisecond))
				if err := ws.WriteMessage(websocket.BinaryMessage, pcm); err != nil {
					writeErr <- err
					cancel()
					return
				}
			}
		}
	}()

	select {
	case <-ctx.Done():
		if parent.Err() != nil {
			return parent.Err()
		}
		return nil
	case err := <-readErr:
		if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
			return nil
		}
		return err
	case err := <-writeErr:
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	case err := <-captureErr:
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if f, err := os.OpenFile("/data/opt/quasar-bridge/lite-live-agent.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644); err == nil {
		defer f.Close()
		log.SetOutput(io.MultiWriter(os.Stderr, f))
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client := &http.Client{Timeout: 800 * time.Millisecond}
	modeCache := newAssistantModeCache()
	go monitorAssistantMode(ctx, client, modeCache)
	nativeVAD := newNativeVADState()
	go monitorNativeYandexVAD(ctx, nativeVAD)
	log.Printf("Yandex Lite mode router started; default=Alice, speech mode=Codex GPT-Live, alternative=Quasar")

	for ctx.Err() == nil {
		if waitForStockWake(ctx, modeCache) && modeCache.load(time.Now()) == "codex" {
			log.Printf("stock wake delegated to Codex GPT-Live")
			if err := runSession(ctx, client, nativeVAD); err != nil && ctx.Err() == nil {
				log.Printf("Codex voice session ended: %v", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(standbyRearmDelay):
		}
	}
}
