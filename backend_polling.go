package fsnotify

import (
	"context"
	"errors"
	"fmt"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/panjf2000/ants/v2"
	"io"
	"io/fs"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	DefaultNumPools  = 150
	DefaultPoolSize  = 1_000
	DefaultBatchSize = 100
)

// Sync operation -> will go through only the root files and check for untracked files
type (
	// Path represents the physical address for some item
	Path string

	// PollingOptions allows you to override the default values for the polling watcher
	// and tune/adjust it to your resource needs
	PollingOptions struct {

		// NumPools represent then number of pools (workers) available for
		// processing tasks (watching dirs, new files...)
		NumPools int

		// PoolSize is the number of tasks each worker can perform (total NumPools * PoolSize)
		PoolSize int

		// BatchSize is the number of items that a watch task can hold.
		// A watch task keep checking for items (files/folders) events
		// this value can be tuned in order to achieve better memory/performance
		BatchSize int
	}

	// PollingOption holds new watcher optional function.
	PollingOption func(opts *PollingOptions)

	basicInfo struct {
		name    string
		size    int64
		mode    fs.FileMode
		modTime time.Time
		isDir   bool
	}

	// item holds the basic info for the current and last version of a file or folder
	item struct {
		current *basicInfo
		old     *basicInfo

		fullPath Path
	}

	polling struct {
		self *item

		// ctx is the context to ensure well behavior for goroutines
		ctx context.Context

		// Events is a channel with the events received by this item
		Events chan Event
		// Errors is a channel with the errors received by this item
		Errors chan error

		mu sync.Mutex

		dirs  []*polling
		files []*item

		// reference to parent node
		parent *polling

		// bloomFilter for quick check if some path is untracked
		bloomFilter *bloom.BloomFilter

		closed bool
		// mark when context done reach listening
		done bool

		cancel context.CancelFunc

		// reference to the watcher, who holds the workers and options
		watcher *pollingWatcher
	}

	pollingWatcher struct {
		id string

		itemWatchers *ants.MultiPool
		dirWatchers  *ants.MultiPool

		options *PollingOptions
	}
)

func toBasicInfo(fi fs.FileInfo) *basicInfo {
	if fi == nil {
		return nil
	}

	return &basicInfo{
		name:    fi.Name(),
		size:    fi.Size(),
		mode:    fi.Mode(),
		modTime: fi.ModTime(),
		isDir:   fi.IsDir(),
	}
}

// WithNumPools returns options with defined number of pools
func WithNumPools(n int) PollingOption {
	return func(opts *PollingOptions) { opts.NumPools = n }
}

// WithPoolSize returns options with defined pool size
func WithPoolSize(n int) PollingOption {
	return func(opts *PollingOptions) { opts.PoolSize = n }
}

// WithBatchSize returns options with defined batch size
func WithBatchSize(n int) PollingOption {
	return func(opts *PollingOptions) { opts.BatchSize = n }
}

func (p *pollingWatcher) loadPollingOptions(options ...PollingOption) {
	opts := &PollingOptions{
		NumPools:  DefaultNumPools,
		PoolSize:  DefaultPoolSize,
		BatchSize: DefaultBatchSize,
	}

	for _, option := range options {
		option(opts)
	}

	p.options = opts
}

func (p *pollingWatcher) startWorkers() error {
	var err error
	p.dirWatchers, err = ants.NewMultiPool(
		p.options.NumPools,
		p.options.PoolSize,
		ants.RoundRobin,
		ants.WithExpiryDuration(1*time.Second),
	)

	if err != nil {
		return err
	}

	p.itemWatchers, err = ants.NewMultiPool(
		p.options.NumPools,
		p.options.PoolSize,
		ants.RoundRobin,
		ants.WithExpiryDuration(1*time.Second),
	)
	return err
}

func newPolling(ctx context.Context, ev chan Event, errs chan error, watcher *pollingWatcher) (*polling, error) {
	ctx, cancel := context.WithCancel(ctx)
	return &polling{
		ctx:         ctx,
		self:        nil,
		Events:      ev,
		Errors:      errs,
		mu:          sync.Mutex{},
		dirs:        make([]*polling, 0, watcher.options.BatchSize),
		files:       make([]*item, 0, watcher.options.BatchSize),
		bloomFilter: bloom.NewWithEstimates(uint(watcher.options.BatchSize), 0.01),
		cancel:      cancel,
		watcher:     watcher,
	}, nil
}

func newPollingWatcher(ctx context.Context, ev chan Event, errs chan error, options ...PollingOption) (*polling, error) {
	watcher := &pollingWatcher{}
	watcher.loadPollingOptions(options...)

	err := watcher.startWorkers()
	if err != nil {
		return nil, err
	}

	return newPolling(ctx, ev, errs, watcher)
}

func (w *polling) add(ctx context.Context, entry os.FileInfo, startWatching bool) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if entry.IsDir() {
		return errors.New("not a file")
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	info := toBasicInfo(entry)
	fullPath := filepath.Join(string(w.self.fullPath), info.name)

	w.bloomFilter.AddString(fullPath)
	item := &item{
		current:  info,
		old:      nil,
		fullPath: Path(fullPath),
	}

	w.files = append(
		w.files,
		item,
	)

	if startWatching {
		err := w.watcher.itemWatchers.Submit(func() {
			w.watchItem(item)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *polling) scan(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	dirBuffer, err := os.Open(string(w.self.fullPath))
	if err != nil {
		return err
	}
	defer func(dirBuffer *os.File) {
		err := dirBuffer.Close()
		if err != nil {
			log.Panic(err)
		}
	}(dirBuffer)

	for {
		entries, err := dirBuffer.ReadDir(w.watcher.options.BatchSize)
		if err == io.EOF {
			return w.watch(ctx)
		}
		if err != nil {
			w.Errors <- err
			continue
		}

		err = w.processBatch(ctx, entries)
		if err != nil {
			w.Errors <- err
		}
	}
}

func (w *polling) processBatch(ctx context.Context, entries []os.DirEntry) error {
	for _, entry := range entries {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		entryInfo, err := entry.Info()
		if err != nil {
			w.Errors <- err
			continue
		}

		if entryInfo.IsDir() {
			p, err := w.addDir(ctx, entryInfo)
			if err != nil {
				w.Errors <- err
				continue
			}

			err = w.watcher.dirWatchers.Submit(func() {
				err := p.scan(ctx)
				if err != nil {
					w.Errors <- err
				}
			})

			continue
		}

		err = w.add(ctx, entryInfo, false)
		if err != nil {
			w.Errors <- err
		}
	}
	return nil
}

func (w *polling) addDir(ctx context.Context, info os.FileInfo) (*polling, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if info.IsDir() == false {
		return nil, errors.New("not a directory")
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	currentInfo := toBasicInfo(info)

	p, err := newPolling(w.ctx, make(chan Event, 500), make(chan error, 500), w.watcher)
	if err != nil {
		return nil, err
	}

	p.self = &item{
		current:  currentInfo,
		old:      nil,
		fullPath: Path(filepath.Join(string(w.self.fullPath), currentInfo.name)),
	}
	p.parent = w

	w.bloomFilter.AddString(string(p.self.fullPath))
	w.dirs = append(w.dirs, p)
	return p, nil
}

func (w *polling) Add(name string) error {
	name = filepath.Clean(name)
	dir := filepath.Dir(name)

	if dir == name && filepath.IsLocal(name) == false {
		l := strings.Split(name, string(filepath.Separator))
		if len(l) < 2 {
			return errors.New("invalid path")
		}

		// adjust path
		dir = filepath.Join(l[:len(l)-1]...)
		for _, e := range l {
			if len(e) != 0 {
				break

			}
			dir = string(filepath.Separator) + dir
		}
	}

	w.self = &item{
		current:  nil,
		old:      nil,
		fullPath: Path(dir),
	}

	defer func() {
		w.self = nil
	}()

	info, err := os.Stat(name)
	if err != nil {
		return err
	}

	if info.IsDir() {
		p, err := w.addDir(w.ctx, info)

		if err != nil {
			return err
		}

		err = p.scan(w.ctx)
		if err != nil {
			return err
		}

		return nil
	}

	return w.add(w.ctx, info, true)
}

func (w *polling) processDiff(_ context.Context, item *item, latest *basicInfo) {
	result := diff(
		item.current,
		latest,
	)
	old := item.current
	item.old, item.current = old, latest

	if result != 0 {
		e := Event{
			Name: string(item.fullPath),
			Op:   result,
		}

		w.Events <- e
		w.applyEvent(e)
	}
}

func (w *polling) watchItem(item *item) {
	if w.ctx.Err() != nil || item == nil || item.current == nil {
		return
	}

	latest, err := os.Stat(string(item.fullPath))
	if err != nil {
		latest = nil
	}

	w.processDiff(w.ctx, item, toBasicInfo(latest))
}

func (w *polling) watchChildren() {
	if w.ctx.Err() != nil {
		return
	}

	//fmt.Println("free", w.watcher.itemWatchers.Free(), "running", w.watcher.itemWatchers.Running())
	for k, dir := range w.dirs {
		if dir == nil {
			continue
		}

		w.watchItem(dir.self)
		if dir.self.current == nil {
			dir.closeDir(w.ctx)
			w.dirs[k] = nil
		}
	}

	for k, file := range w.files {
		if file == nil {
			continue
		}

		w.watchItem(file)
		if file.current == nil {
			w.files[k] = nil
		}
	}

	err := w.watcher.itemWatchers.Submit(func() {
		time.Sleep(time.Duration(rand.Int31n(1000)+10) * time.Millisecond)
		w.watchChildren()
	})
	if err != nil && !w.closed && w.ctx.Err() == nil {
		w.Errors <- err
	}
}

func (w *polling) checkNewItemsBatch(entries []os.DirEntry) {
	for _, entry := range entries {
		fullPath := filepath.Join(string(w.self.fullPath), entry.Name())
		tracked := w.bloomFilter.TestString(fullPath)

		// definitely not tracked
		if !tracked {
			w.Events <- Event{
				Name: fullPath,
				Op:   Create,
			}

			// start tracking
			info, err := entry.Info()
			if err != nil {
				w.Errors <- err
				continue
			}

			if entry.IsDir() {
				p, err := w.addDir(w.ctx, info)
				if err != nil {
					w.Errors <- err
					continue
				}
				err = w.watcher.dirWatchers.Submit(func() {
					err := p.scan(w.ctx)
					if err != nil {
						w.Errors <- err
					}
				})
				if err != nil {
					w.Errors <- err
				}
				continue
			}

			err = w.add(w.ctx, info, true)
			if err != nil {
				w.Errors <- err
				continue
			}
		}
	}
}

func (w *polling) watchForNewItems() {
	if w.ctx.Err() != nil {
		return
	}

	dirBuffer, err := os.Open(string(w.self.fullPath))
	if err != nil {
		w.Errors <- err
		return
	}
	defer func(dirBuffer *os.File) {
		err := dirBuffer.Close()
		if err != nil && !w.closed {
			w.Errors <- err
		}
	}(dirBuffer)

	for {
		// process directory looking for new items by batches
		entries, err := dirBuffer.ReadDir(w.watcher.options.BatchSize)
		if err == io.EOF {
			break
		}
		if err != nil {
			w.Errors <- err
			continue
		}

		w.checkNewItemsBatch(entries)
	}

	err = w.watcher.dirWatchers.Submit(func() {
		time.Sleep(time.Duration(rand.Int31n(150)+100) * time.Millisecond)
		w.watchForNewItems()
	})
	if err != nil && !w.closed {
		w.Errors <- err
	}
}

func (w *polling) redoBloomFilter() {
	w.mu.Lock()
	defer w.mu.Unlock()

	newBloom := bloom.NewWithEstimates(uint(w.watcher.options.BatchSize), 0.01)
	for _, dir := range w.dirs {
		if dir == nil || dir.self == nil || dir.self.current == nil {
			continue
		}
		newBloom.AddString(string(dir.self.fullPath))
	}

	for _, file := range w.files {
		if file == nil || file.current == nil {
			continue
		}
		newBloom.AddString(string(file.fullPath))
	}

	w.bloomFilter = newBloom
}

func (w *polling) applyEvent(event Event) {
	if event.Op == Remove {
		w.redoBloomFilter()
		return
	}
}

func (w *polling) listen() {
	for {
		select {
		case <-w.ctx.Done():
			w.done = true
			return
		case e := <-w.Events:
			w.parent.Events <- e
		case e := <-w.Errors:
			w.parent.Errors <- e
		}
	}
}

func (w *polling) watch(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// if not root
	if w.self != nil {
		err := w.watcher.dirWatchers.Submit(func() {
			w.watchForNewItems()
		})
		if err != nil {
			return err
		}

		// no need for pool in this persistent routine
		go func() {
			w.listen()
		}()
	}

	err := w.watcher.itemWatchers.Submit(func() {
		w.watchChildren()
	})
	if err != nil {
		w.Errors <- err
	}
	return err
}

func diff(a, b *basicInfo) Op {
	if a == nil {
		fmt.Println("This should not happen")
		return 0
	}

	if b == nil {
		return Remove
	}

	if a.mode != b.mode {
		return Chmod
	}

	if a.modTime != b.modTime {
		return Write
	}

	if a.isDir == false && a.size != b.size {
		return Write
	}

	return 0
}

func (w *polling) closeDir(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	w.closed = true
	if w.ctx.Err() == nil {
		w.cancel()
	}

	for _, entry := range w.dirs {
		if entry == nil {
			continue
		}
		entry.closeDir(ctx)
	}

	if w.ctx.Err() == nil {
		for {
			if ctx.Err() != nil {
				return
			}
			if w.done {
				break
			}
		}
	}

	w.parent = nil
	close(w.Errors)
	close(w.Events)
}

func (w *polling) close(ctx context.Context) error {
	ch := make(chan struct{})
	defer close(ch)

	go func() {
		w.cancel()
		for {
			if ctx.Err() != nil {
				return
			}

			if w.watcher.itemWatchers.Running() == 0 && w.watcher.dirWatchers.Running() == 0 {
				ch <- struct{}{}
				break
			}
		}

		w.closeDir(ctx)
		if ctx.Err() != nil {
			return
		}
		ch <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		<-ch
		return nil
	}
}

func (w *polling) Close() error {
	if w.closed {
		return ErrClosed
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w.closed = true

	err := w.close(ctx)

	w.dirs = nil
	w.files = nil
	w.bloomFilter = nil

	if err != nil {
		return err
	}
	return nil
}

func (w *polling) WatchList() []string {
	if w.closed {
		return nil
	}

	var out []string

	for _, dir := range w.dirs {
		out = append(out, string(dir.self.fullPath))
	}

	for _, f := range w.files {
		out = append(out, string(f.fullPath))
	}

	return out
}

func (w *polling) removeDir(path Path) error {
	for i, dir := range w.dirs {
		if dir.self.fullPath == path {
			w.mu.Lock()
			w.dirs = append(w.dirs[:i], w.dirs[i+1:]...)
			w.mu.Unlock()

			dir.closeDir(w.ctx)

			return nil
		}
	}

	return ErrNonExistentWatch
}

func (w *polling) removeFile(path Path) error {
	for i, file := range w.files {
		if file.fullPath == path {
			w.mu.Lock()
			w.files = append(w.files[:i], w.files[i+1:]...)
			w.mu.Unlock()
			return nil
		}
	}
	return ErrNonExistentWatch
}

func (w *polling) Remove(name string) error {
	if w.closed {
		return ErrClosed
	}

	path := filepath.Clean(name)
	info, err := os.Stat(path)
	if err != nil {
		return err
	}

	if info.IsDir() {
		return w.removeDir(Path(path))
	}
	return w.removeFile(Path(path))
}

func (w *polling) AddWith(name string, opts ...addOpt) error { return nil }
func (w *polling) xSupports(op Op) bool                      { return false }

/*
TODO
[ ] Recover memory from deletions
[ ] Add with - options for interval
[ ] xSupports
[ ] Review watch for new items - add to existent batch
*/
