package handles

import (
	"fmt"
	"io"
	"net/url"
	"os"
	stdpath "path"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/internal/task"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

type chunkUploadState struct {
	mu             sync.Mutex
	totalChunks    int
	receivedChunks map[int]bool
	chunkDir       string
	createdAt      time.Time
}

var (
	chunkUploads      = make(map[string]*chunkUploadState)
	chunkUploadsMutex sync.Mutex
)

func init() {
	go cleanupExpiredChunkUploads()
}

func cleanupExpiredChunkUploads() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		chunkUploadsMutex.Lock()
		for id, state := range chunkUploads {
			if time.Since(state.createdAt) > 24*time.Hour {
				_ = os.RemoveAll(state.chunkDir)
				delete(chunkUploads, id)
				log.Debugf("cleaned up expired chunk upload: %s", id)
			}
		}
		chunkUploadsMutex.Unlock()
	}
}

func getOrCreateChunkState(uploadID string, totalChunks int) (*chunkUploadState, error) {
	chunkUploadsMutex.Lock()
	defer chunkUploadsMutex.Unlock()

	if state, exists := chunkUploads[uploadID]; exists {
		return state, nil
	}

	chunkDir, err := os.MkdirTemp(conf.Conf.TempDir, "chunk-upload-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create chunk directory: %w", err)
	}

	state := &chunkUploadState{
		totalChunks:    totalChunks,
		receivedChunks: make(map[int]bool),
		chunkDir:       chunkDir,
		createdAt:      time.Now(),
	}
	chunkUploads[uploadID] = state
	return state, nil
}

func removeChunkState(uploadID string) {
	chunkUploadsMutex.Lock()
	defer chunkUploadsMutex.Unlock()
	if state, exists := chunkUploads[uploadID]; exists {
		if err := os.RemoveAll(state.chunkDir); err != nil {
			log.Errorf("failed to remove chunk directory %s: %v", state.chunkDir, err)
		}
		delete(chunkUploads, uploadID)
	}
}

func FsStreamChunked(c *gin.Context) {
	defer func() {
		if n, _ := io.ReadFull(c.Request.Body, []byte{0}); n == 1 {
			_, _ = utils.CopyWithBuffer(io.Discard, c.Request.Body)
		}
		_ = c.Request.Body.Close()
	}()

	path := c.GetHeader("File-Path")
	path, err := url.PathUnescape(path)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}

	uploadID := c.GetHeader("X-Upload-Id")
	if uploadID == "" {
		common.ErrorStrResp(c, "X-Upload-Id header is required", 400)
		return
	}

	chunkNumberStr := c.GetHeader("X-Chunk-Number")
	totalChunksStr := c.GetHeader("X-Total-Chunks")
	if chunkNumberStr == "" || totalChunksStr == "" {
		common.ErrorStrResp(c, "X-Chunk-Number and X-Total-Chunks headers are required", 400)
		return
	}

	chunkNumber, err := strconv.Atoi(chunkNumberStr)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	totalChunks, err := strconv.Atoi(totalChunksStr)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}

	if chunkNumber < 0 || chunkNumber >= totalChunks || totalChunks <= 0 {
		common.ErrorStrResp(c, "invalid chunk number or total chunks", 400)
		return
	}

	asTask := c.GetHeader("As-Task") == "true"
	overwrite := c.GetHeader("Overwrite") != "false"
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	path, err = user.JoinPath(path)
	if err != nil {
		common.ErrorResp(c, err, 403)
		return
	}

	if !overwrite {
		if res, _ := fs.Get(c.Request.Context(), path, &fs.GetArgs{NoLog: true}); res != nil {
			common.ErrorStrResp(c, "file exists", 403)
			return
		}
	}

	dir, name := stdpath.Split(path)
	if shouldIgnoreSystemFile(name) {
		common.ErrorStrResp(c, errs.IgnoredSystemFile.Error(), 403)
		return
	}

	// Get or create chunk upload state
	state, err := getOrCreateChunkState(uploadID, totalChunks)
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}

	// Save this chunk to a temp file
	chunkPath := filepath.Join(state.chunkDir, fmt.Sprintf("%d", chunkNumber))
	chunkFile, err := os.Create(chunkPath)
	if err != nil {
		common.ErrorResp(c, fmt.Errorf("failed to create chunk file: %w", err), 500)
		return
	}
	defer chunkFile.Close()

	_, err = utils.CopyWithBuffer(chunkFile, c.Request.Body)
	if err != nil {
		common.ErrorResp(c, fmt.Errorf("failed to write chunk: %w", err), 500)
		return
	}

	// Mark chunk as received
	state.mu.Lock()
	state.receivedChunks[chunkNumber] = true
	allReceived := len(state.receivedChunks) == state.totalChunks
	state.mu.Unlock()

	if !allReceived {
		// Not all chunks received yet, return success for this chunk
		common.SuccessResp(c, gin.H{
			"uploaded": chunkNumber,
		})
		return
	}

	// All chunks received, assemble the file
	assembledFile, totalSize, err := assembleChunks(state)
	if err != nil {
		removeChunkState(uploadID)
		common.ErrorResp(c, fmt.Errorf("failed to assemble chunks: %w", err), 500)
		return
	}

	// Read file size header if provided
	fileSizeStr := c.GetHeader("X-File-Size")
	if fileSizeStr != "" {
		fileSize, parseErr := strconv.ParseInt(fileSizeStr, 10, 64)
		if parseErr == nil {
			totalSize = fileSize
		}
	}

	h := make(map[*utils.HashType]string)
	if md5 := c.GetHeader("X-File-Md5"); md5 != "" {
		h[utils.MD5] = md5
	}
	if sha1 := c.GetHeader("X-File-Sha1"); sha1 != "" {
		h[utils.SHA1] = sha1
	}
	if sha256 := c.GetHeader("X-File-Sha256"); sha256 != "" {
		h[utils.SHA256] = sha256
	}

	mimetype := c.GetHeader("Content-Type")
	if len(mimetype) == 0 {
		mimetype = utils.GetMimeType(name)
	}

	s := &stream.FileStream{
		Obj: &model.Object{
			Name:     name,
			Size:     totalSize,
			Modified: getLastModified(c),
			HashInfo: utils.NewHashInfoByMap(h),
		},
		Reader:       assembledFile,
		Mimetype:     mimetype,
		WebPutAsTask: asTask,
	}
	s.Add(utils.CloseFunc(func() error {
		removeChunkState(uploadID)
		name := assembledFile.Name()
		err := assembledFile.Close()
		_ = os.Remove(name)
		return err
	}))

	var t task.TaskExtensionInfo
	if asTask {
		t, err = fs.PutAsTask(c.Request.Context(), dir, s)
	} else {
		err = fs.PutDirectly(c.Request.Context(), dir, s)
	}
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	if t == nil {
		common.SuccessResp(c)
		return
	}
	common.SuccessResp(c, gin.H{
		"task": getTaskInfo(t),
	})
}

// assembleChunks combines all chunk files into a single temporary file
func assembleChunks(state *chunkUploadState) (*os.File, int64, error) {
	assembledFile, err := os.CreateTemp(conf.Conf.TempDir, "assembled-*")
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create assembled file: %w", err)
	}

	cleanupAssembled := func() {
		if err := assembledFile.Close(); err != nil {
			log.Errorf("failed to close assembled file: %v", err)
		}
		if err := os.Remove(assembledFile.Name()); err != nil {
			log.Errorf("failed to remove assembled file %s: %v", assembledFile.Name(), err)
		}
	}

	var totalSize int64
	for i := 0; i < state.totalChunks; i++ {
		chunkPath := filepath.Join(state.chunkDir, fmt.Sprintf("%d", i))
		chunkFile, err := os.Open(chunkPath)
		if err != nil {
			cleanupAssembled()
			return nil, 0, fmt.Errorf("failed to open chunk %d: %w", i, err)
		}
		n, err := utils.CopyWithBuffer(assembledFile, chunkFile)
		chunkFile.Close()
		if err != nil {
			cleanupAssembled()
			return nil, 0, fmt.Errorf("failed to copy chunk %d: %w", i, err)
		}
		totalSize += n
	}

	_, err = assembledFile.Seek(0, io.SeekStart)
	if err != nil {
		cleanupAssembled()
		return nil, 0, fmt.Errorf("failed to seek assembled file: %w", err)
	}

	return assembledFile, totalSize, nil
}
