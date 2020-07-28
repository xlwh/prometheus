// Copyright 2017 The Prometheus Authors

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb/fileutil"
)

const (
	DefaultSegmentSize = 128 * 1024 * 1024 // 128 MB
	pageSize           = 32 * 1024         // 32KB, 一页的大小是32KB
	recordHeaderSize   = 7                 // 7个字节，[ 4 bits unallocated] [1 bit snappy compression flag] [ 3 bit record type ]
)

// The table gets initialized with sync.Once but may still cause a race
// with any other use of the crc32 package anywhere. Thus we initialize it
// before.
var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// page is an in memory buffer used to batch disk writes.    // 内存buffer，方便进行批量写磁盘
// Records bigger than the page size are split and flushed separately.
// A flush is triggered when a single records doesn't fit the page size or
// when the next record can't fit in the remaining free page space.
type page struct {
	alloc   int
	flushed int
	buf     [pageSize]byte // 32KB
}

// 计算剩余大小
func (p *page) remaining() int {
	return pageSize - p.alloc
}

// 判断是否满了，为啥要预留7个，buf 也要留个头???
func (p *page) full() bool {
	return pageSize-p.alloc < recordHeaderSize
}

// 清空buffer
func (p *page) reset() {
	for i := range p.buf {
		p.buf[i] = 0
	}
	p.alloc = 0
	p.flushed = 0
}

// Segment represents a segment file.
// 一个Segment的大小是128M
type Segment struct {
	*os.File        // 文件句柄
	dir      string // 数据保存目录
	i        int    // Segment编号，一直自增的
}

// Index returns the index of the segment.
// 返回当前wal segment的编号，也就是告诉我们现在写到了哪个Segment了
func (s *Segment) Index() int {
	return s.i
}

// Dir returns the directory of the segment.
// wal log的数据保存目录
func (s *Segment) Dir() string {
	return s.dir
}

// CorruptionErr is an error that's returned when corruption is encountered.
// 对数据损坏错误进行一些封装
type CorruptionErr struct {
	Dir     string // 发生错误数据的目录
	Segment int    // Segment编号
	Offset  int64  // 发生错误的数据的位置
	Err     error  // 具体的错误信息
}

// 错误信息封装成字符串返回给上层，方便进行一些记录日志等
func (e *CorruptionErr) Error() string {
	if e.Segment < 0 {
		return fmt.Sprintf("corruption after %d bytes: %s", e.Offset, e.Err)
	}
	return fmt.Sprintf("corruption in segment %s at %d: %s", SegmentName(e.Dir, e.Segment), e.Offset, e.Err)
}

// CreateSegment creates a new segment k in dir.
// 传入文件夹目录和编号，创建一个Segment
func CreateSegment(dir string, k int) (*Segment, error) {
	f, err := os.OpenFile(SegmentName(dir, k), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	// 返回一个Segment对象，或者说是结构体，里面主要放了一个文件句柄FD
	return &Segment{File: f, i: k, dir: dir}, nil
}

// OpenReadSegment opens the segment with the given filename.
// 打开一个Segment，应该是只读的Segment
func OpenReadSegment(fn string) (*Segment, error) {
	// 转成数字
	k, err := strconv.Atoi(filepath.Base(fn))
	if err != nil {
		return nil, errors.New("not a valid filename")
	}
	// os.Open(),默认是只读
	f, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	// 创建一个空的Segment
	return &Segment{File: f, i: k, dir: filepath.Dir(fn)}, nil
}

// WAL is a write ahead log that stores records in segment files.
// It must be read from start to end once before logging new data.
// If an error occurs during read, the repair procedure must be called
// before it's safe to do further writes.
//
// Segments are written to in pages of 32KB, with records possibly split
// across page boundaries.
// Records are never split across segments to allow full segments to be
// safely truncated. It also ensures that torn writes never corrupt records
// beyond the most recent segment.
type WAL struct {
	dir         string             // 数据目录
	logger      log.Logger         // 日志组件,每个地方都要传入一个日志对象，感觉用起来比较麻烦
	segmentSize int                // segment的大小
	mtx         sync.RWMutex       // 互斥锁，多线程安全
	segment     *Segment           // Active segment.    // 当前正在读写的segment
	donePages   int                // Pages written to the segment.
	page        *page              // Active page.      // 当前页
	stopc       chan chan struct{} // 服务停止信号
	actorc      chan func()
	closed      bool   // To allow calling Close() more than once without blocking.
	compress    bool   // 是否进行数据压缩
	snappyBuf   []byte //

	metrics *walMetrics // 监控指标
}

type walMetrics struct {
	fsyncDuration   prometheus.Summary // 数据同步耗时
	pageFlushes     prometheus.Counter // 一共刷了多少页
	pageCompletions prometheus.Counter // 页压缩次数
	truncateFail    prometheus.Counter // 删除失败次数
	truncateTotal   prometheus.Counter // 删除总数
	currentSegment  prometheus.Gauge   // 当前的senment编号
	writesFailed    prometheus.Counter // 写入失败数
}

// 创建和注册监控指标
func newWALMetrics(r prometheus.Registerer) *walMetrics {
	m := &walMetrics{}

	m.fsyncDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Name:       "prometheus_tsdb_wal_fsync_duration_seconds",
		Help:       "Duration of WAL fsync.",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})
	m.pageFlushes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_wal_page_flushes_total",
		Help: "Total number of page flushes.",
	})
	m.pageCompletions = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_wal_completed_pages_total",
		Help: "Total number of completed pages.",
	})
	m.truncateFail = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_wal_truncations_failed_total",
		Help: "Total number of WAL truncations that failed.",
	})
	m.truncateTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_wal_truncations_total",
		Help: "Total number of WAL truncations attempted.",
	})
	m.currentSegment = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_wal_segment_current",
		Help: "WAL segment index that TSDB is currently writing to.",
	})
	m.writesFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_wal_writes_failed_total",
		Help: "Total number of WAL writes that failed.",
	})

	// 注册监控
	if r != nil {
		r.MustRegister(
			m.fsyncDuration,
			m.pageFlushes,
			m.pageCompletions,
			m.truncateFail,
			m.truncateTotal,
			m.currentSegment,
			m.writesFailed,
		)
	}

	return m
}

// New returns a new WAL over the given directory.
// 创建一个新的WAL，用于写入数据,默认的大小是128M
func New(logger log.Logger, reg prometheus.Registerer, dir string, compress bool) (*WAL, error) {
	return NewSize(logger, reg, dir, DefaultSegmentSize, compress)
}

// NewSize returns a new WAL over the given directory.
// New segments are created with the specified size.
// 按照给定的大小，创建WAL
func NewSize(logger log.Logger, reg prometheus.Registerer, dir string, segmentSize int, compress bool) (*WAL, error) {
	// 传入的segmentSize必须是页大小的整数倍，方便进行对齐处理
	// 也就是32K的整数倍，这里有个问题，为什么pageSize必须是32K
	if segmentSize%pageSize != 0 {
		return nil, errors.New("invalid segment size")
	}
	// 创建目录,目录的权限是0777的
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, errors.Wrap(err, "create dir")
	}
	// 如果没创建日志记录器，那么这创建一个空的日志记录器
	if logger == nil {
		// 创建一个空的日志记录器,只是有这个结构而已，实际上它什么都不做的
		logger = log.NewNopLogger()
	}

	// 创建一个WAL对象
	w := &WAL{
		dir:         dir,                      // 数据保存的目录
		logger:      logger,                   // 日志组件
		segmentSize: segmentSize,              // segment的大小，默认是128M
		page:        &page{},                  // 页面
		actorc:      make(chan func(), 100),   // 这个是做什么的？为啥创建100个？
		stopc:       make(chan chan struct{}), // 服务停止信号，当收到这个信号后，就要做一些服务退出的操作，比如把内存中的page flush到disk
		compress:    compress,                 // 是否进行数据的压缩
	}
	// 创建监控
	w.metrics = newWALMetrics(reg)

	// 扫一下对应的数据目录，拿到最后一个segment的ID
	_, last, err := w.Segments()
	if err != nil {
		return nil, errors.Wrap(err, "get segment range")
	}

	// Index of the Segment we want to open and write to.
	writeSegmentIndex := 0
	// If some segments already exist create one with a higher index than the last segment.
	// 每次重启，Segment id都会自增
	if last != -1 {
		writeSegmentIndex = last + 1
	}

	// 创建一个新的segment
	segment, err := CreateSegment(w.dir, writeSegmentIndex)
	if err != nil {
		return nil, err
	}

	if err := w.setSegment(segment); err != nil {
		return nil, err
	}

	// 启动一个后台任务，做一些别的事情，后台任务
	go w.run()

	return w, nil
}

// Open an existing WAL.
// 打开一个已经存在的WAL
func Open(logger log.Logger, dir string) (*WAL, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	w := &WAL{
		dir:    dir,
		logger: logger,
	}

	return w, nil
}

// CompressionEnabled returns if compression is enabled on this WAL.
// 是否允许数据压缩
func (w *WAL) CompressionEnabled() bool {
	return w.compress
}

// Dir returns the directory of the WAL.
// 当前数据保存的目录
func (w *WAL) Dir() string {
	return w.dir
}

// 接收一些后台函数或者任务，调度执行
func (w *WAL) run() {
Loop:
	for {
		select {
		// 发一些信号，这边接收到，就执行动

		case f := <-w.actorc:
			f() // 传入的应该是一个函数，还可以这样玩奥
		case donec := <-w.stopc:
			close(w.actorc)
			defer close(donec)
			break Loop
		}
	}
	// Drain and process any remaining functions.
	// 退出前，也尽量把没执行完的任务执行完
	for f := range w.actorc {
		f()
	}
}

// Repair attempts to repair the WAL based on the error.
// It discards all data after the corruption.
// 传入错误信息尝试修复在数据读取过程中的数据错误
func (w *WAL) Repair(origErr error) error {
	// We could probably have a mode that only discards torn records right around
	// the corruption to preserve as data much as possible.
	// But that's not generally applicable if the records have any kind of causality.
	// Maybe as an extra mode in the future if mid-WAL corruptions become
	// a frequent concern.
	// 损坏，也尽可能保留数据

	// 取出错误信息
	err := errors.Cause(origErr) // So that we can pick up errors even if wrapped.

	// 为啥又转回去？？
	cerr, ok := err.(*CorruptionErr)
	if !ok {
		return errors.Wrap(origErr, "cannot handle error")
	}
	if cerr.Segment < 0 {
		return errors.New("corruption error does not specify position")
	}
	level.Warn(w.logger).Log("msg", "Starting corruption repair",
		"segment", cerr.Segment, "offset", cerr.Offset)

	// All segments behind the corruption can no longer be used.
	// 列出目录下面的所有的segment文件名称和id,返回的是有序的
	segs, err := listSegments(w.dir)
	if err != nil {
		return errors.Wrap(err, "list segments")
	}

	level.Warn(w.logger).Log("msg", "Deleting all segments newer than corrupted segment", "segment", cerr.Segment)

	// 遍历每个segment,然后做两个事情
	// 1.关闭最近活跃中的Segment  2.删除超前的Segment
	for _, s := range segs {
		// 把当前活跃的segment关闭
		if w.segment.i == s.index {
			// The active segment needs to be removed,
			// close it first (Windows!). Can be closed safely
			// as we set the current segment to repaired file
			// below.
			if err := w.segment.Close(); err != nil {
				return errors.Wrap(err, "close active segment")
			}
		}
		// 异常的segment 小于等于当前活跃的，不用处理
		if s.index <= cerr.Segment {
			continue
		}

		// 删除超前的segment
		// 什么情况下会有这种超前的Segment呢？？
		if err := os.Remove(filepath.Join(w.dir, s.name)); err != nil {
			return errors.Wrapf(err, "delete segment:%v", s.index)
		}
	}

	//	不管损坏的偏移量如何，都没有记录到达上一个段
	//	因此，我们可以通过删除段并重新插入所有WAL记录直至损坏来安全地修复WAL。
	level.Warn(w.logger).Log("msg", "Rewrite corrupted segment", "segment", cerr.Segment)

	// 修复后的文件名 .repair
	fn := SegmentName(w.dir, cerr.Segment)
	tmpfn := fn + ".repair"

	// 把损坏的文件改一个名字
	if err := fileutil.Rename(fn, tmpfn); err != nil {
		return err
	}

	// Create a clean segment and make it the active one.
	// 创建一个新的Segment
	s, err := CreateSegment(w.dir, cerr.Segment)
	if err != nil {
		return err
	}
	if err := w.setSegment(s); err != nil {
		return err
	}

	// 打开老的损坏的那个Segment
	f, err := os.Open(tmpfn)
	if err != nil {
		return errors.Wrap(err, "open segment")
	}
	defer f.Close()

	r := NewReader(bufio.NewReader(f))

	// 读取数据，跳过损坏的那个数据，然后重新写入
	// 只写损坏段以前的数据,这里其实会丢一些数据
	for r.Next() {
		// Add records only up to the where the error was.
		if r.Offset() >= cerr.Offset {
			break
		}
		if err := w.Log(r.Record()); err != nil {
			return errors.Wrap(err, "insert record")
		}
	}
	// We expect an error here from r.Err(), so nothing to handle.

	// We need to pad to the end of the last page in the repaired segment
	// 刷出数据
	if err := w.flushPage(true); err != nil {
		return errors.Wrap(err, "flush page in repair")
	}

	// We explicitly close even when there is a defer for Windows to be
	// able to delete it. The defer is in place to close it in-case there
	// are errors above.
	// 关闭和清理错误的Segment
	if err := f.Close(); err != nil {
		return errors.Wrap(err, "close corrupted file")
	}
	if err := os.Remove(tmpfn); err != nil {
		return errors.Wrap(err, "delete corrupted segment")
	}

	// Explicitly close the segment we just repaired to avoid issues with Windows.
	s.Close()

	//我们始终希望开始写入新的细分，而不是现有的细分
	//段，该段由NewSize处理，但在修复之前，我们要删除
	//损坏的细分之后的所有细分。 在此处重新创建一个新的细分。
	s, err = CreateSegment(w.dir, cerr.Segment+1)
	if err != nil {
		return err
	}
	if err := w.setSegment(s); err != nil {
		return err
	}
	return nil
}

// SegmentName builds a segment name for the directory.
// 给定路径和文件名，拼接完整的文件路径
func SegmentName(dir string, i int) string {
	return filepath.Join(dir, fmt.Sprintf("%08d", i))
}

// NextSegment creates the next segment and closes the previous one.
// 创建新的segment，并关闭当前的segment
func (w *WAL) NextSegment() error {
	// 这里需要加锁吗？？
	w.mtx.Lock()
	defer w.mtx.Unlock()
	return w.nextSegment()
}

// nextSegment creates the next segment and closes the previous one.
// 关闭当前的segment，开启下一个segment
func (w *WAL) nextSegment() error {
	// Only flush the current page if it actually holds data.
	// 当前的内存页里面有数据，那么要先把内存中的数据刷出去
	if w.page.alloc > 0 {
		if err := w.flushPage(true); err != nil {
			return err
		}
	}
	// 创建一个新的segment
	next, err := CreateSegment(w.dir, w.segment.Index()+1)
	if err != nil {
		return errors.Wrap(err, "create new segment file")
	}
	prev := w.segment
	if err := w.setSegment(next); err != nil {
		return err
	}

	// Don't block further writes by fsyncing the last segment.
	// 把前一个数据同步一下，这块为了不阻塞，所以这样设计
	w.actorc <- func() {
		if err := w.fsync(prev); err != nil {
			level.Error(w.logger).Log("msg", "sync previous segment", "err", err)
		}
		if err := prev.Close(); err != nil {
			level.Error(w.logger).Log("msg", "close previous segment", "err", err)
		}
	}
	return nil
}

// 设置当前正在接受读写的segment
func (w *WAL) setSegment(segment *Segment) error {
	w.segment = segment // 当前的segment

	// Correctly initialize donePages.
	// 返回当前的segment文件的状态
	stat, err := segment.Stat()
	if err != nil {
		return err
	}
	// 当前文件已经写了多少页？？
	w.donePages = int(stat.Size() / pageSize)
	// 更新一下监控信息
	w.metrics.currentSegment.Set(float64(segment.Index()))
	return nil
}

// flushPage将页面的新内容写入磁盘。 如果没有更多的记录适合该页面，剩余字节将被设置为零，然后将开始一个新页面,数据在页面上不会被截断
// 如果clear为true，则无论页面中剩余多少字节，都将强制执行此操作。
func (w *WAL) flushPage(clear bool) error {
	w.metrics.pageFlushes.Inc()

	p := w.page
	clear = clear || p.full()

	// No more data will fit into the page or an implicit clear.
	// Enqueue and clear it.
	if clear {
		p.alloc = pageSize // Write till end of page.
	}
	// 从上次刷出后面刷剩余的数据
	n, err := w.segment.Write(p.buf[p.flushed:p.alloc])
	if err != nil {
		return err
	}
	p.flushed += n

	// We flushed an entire page, prepare a new one.
	if clear {
		p.reset()
		w.donePages++
		w.metrics.pageCompletions.Inc()
	}
	return nil
}

// First Byte of header format:
// [ 4 bits unallocated] [1 bit snappy compression flag] [ 3 bit record type ]
const (
	snappyMask  = 1 << 3
	recTypeMask = snappyMask - 1
)

type recType uint8

const (
	recPageTerm recType = 0 // Rest of page is empty.
	recFull     recType = 1 // Full record.
	recFirst    recType = 2 // First fragment of a record.
	recMiddle   recType = 3 // Middle fragments of a record.
	recLast     recType = 4 // Final fragment of a record.
)

func recTypeFromHeader(header byte) recType {
	return recType(header & recTypeMask)
}

func (t recType) String() string {
	switch t {
	case recPageTerm:
		return "zero"
	case recFull:
		return "full"
	case recFirst:
		return "first"
	case recMiddle:
		return "middle"
	case recLast:
		return "last"
	default:
		return "<invalid>"
	}
}

// 计算一些每个segment里面有多少个page
func (w *WAL) pagesPerSegment() int {
	return w.segmentSize / pageSize
}

// Log writes the records into the log.
// Multiple records can be passed at once to reduce writes and increase throughput.
// 写日志,一次可以写多条数据
func (w *WAL) Log(recs ...[]byte) error {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	// Callers could just implement their own list record format but adding
	// a bit of extra logic here frees them from that overhead.
	for i, r := range recs {
		// 写入数据
		if err := w.log(r, i == len(recs)-1); err != nil {
			w.metrics.writesFailed.Inc()
			return err
		}
	}
	return nil
}

// log writes rec to the log and forces a flush of the current page if:
// 以下的几种情况，会强制刷页
// - the final record of a batch						   	一个批量的最后一条数据
// - the record is bigger than the page size				写入的数据大于页的大小，24K
// - the current page is full.								当前页已经写满了
func (w *WAL) log(rec []byte, final bool) error {
	// When the last page flush failed the page will remain full.
	// When the page is full, need to flush it before trying to add more records to it.
	// 当页面已满时，需要先刷新页面，然后再尝试向其中添加更多记录
	if w.page.full() {
		if err := w.flushPage(true); err != nil {
			return err
		}
	}
	// If the record is too big to fit within the active page in the current
	// segment, terminate the active segment and advance to the next one.
	// This ensures that records do not cross segment boundaries.
	// 计算剩余的大小，需要预留head的7个字节
	left := w.page.remaining() - recordHeaderSize                                   // Free space in the active page.
	left += (pageSize - recordHeaderSize) * (w.pagesPerSegment() - w.donePages - 1) // Free pages in the active segment.

	// 剩余的页不够装一条数据，那么就直接下一个page
	// 在page中不需要对data进行拆分，避免数据损坏发生
	if len(rec) > left {
		if err := w.nextSegment(); err != nil {
			return err
		}
	}

	// 看看是否要对数据进行压缩， 如果开启了压缩的话，把写入的数据用snappy进行压缩
	compressed := false
	if w.compress && len(rec) > 0 {
		// The snappy library uses `len` to calculate if we need a new buffer.
		// In order to allocate as few buffers as possible make the length
		// equal to the capacity.
		w.snappyBuf = w.snappyBuf[:cap(w.snappyBuf)]
		w.snappyBuf = snappy.Encode(w.snappyBuf, rec)
		if len(w.snappyBuf) < len(rec) {
			rec = w.snappyBuf
			compressed = true
		}
	}

	// Populate as many pages as necessary to fit the record.
	// Be careful to always do one pass to ensure we write zero-length records.
	for i := 0; i == 0 || len(rec) > 0; i++ {
		p := w.page

		// Find how much of the record we can fit into the page.
		var (
			// 剩余空间大小和要写入的数据，取一个最小值
			l = min(len(rec), (pageSize-p.alloc)-recordHeaderSize)
			// 取出一段，适配buf
			part = rec[:l]
			// 页面中剩余的空间
			buf = p.buf[p.alloc:]
			typ recType
		)

		switch {
		case i == 0 && len(part) == len(rec):
			typ = recFull // 全放下了
		case len(part) == len(rec):
			typ = recLast
		case i == 0:
			typ = recFirst
		default:
			typ = recMiddle
		}
		if compressed {
			typ |= snappyMask
		}

		/*
			┌───────────┬──────────┬────────────┬──────────────┐
			│ type <1b> │ len <2b> │ CRC32 <4b> │ data <bytes> │
			└───────────┴──────────┴────────────┴──────────────┘
		*/
		// 写type 1个字节
		buf[0] = byte(typ)
		crc := crc32.Checksum(part, castagnoliTable)
		// 两个字节，数据长度
		binary.BigEndian.PutUint16(buf[1:], uint16(len(part)))
		// 四个字节，放CRC，32位
		binary.BigEndian.PutUint32(buf[3:], crc)

		// 放数据 data
		copy(buf[recordHeaderSize:], part)
		p.alloc += len(part) + recordHeaderSize

		if w.page.full() {
			if err := w.flushPage(true); err != nil {
				return err
			}
		}
		rec = rec[l:]
	}

	// If it's the final record of the batch and the page is not empty, flush it.
	if final && w.page.alloc > 0 {
		if err := w.flushPage(false); err != nil {
			return err
		}
	}

	return nil
}

// Segments returns the range [first, n] of currently existing segments.
// If no segments are found, first and n are -1.
func (w *WAL) Segments() (first, last int, err error) {
	refs, err := listSegments(w.dir)
	if err != nil {
		return 0, 0, err
	}
	if len(refs) == 0 {
		return -1, -1, nil
	}
	return refs[0].index, refs[len(refs)-1].index, nil
}

// Truncate drops all segments before i.
func (w *WAL) Truncate(i int) (err error) {
	w.metrics.truncateTotal.Inc()
	defer func() {
		if err != nil {
			w.metrics.truncateFail.Inc()
		}
	}()
	refs, err := listSegments(w.dir)
	if err != nil {
		return err
	}
	for _, r := range refs {
		if r.index >= i {
			break
		}
		if err = os.Remove(filepath.Join(w.dir, r.name)); err != nil {
			return err
		}
	}
	return nil
}

func (w *WAL) fsync(f *Segment) error {
	start := time.Now()
	err := f.File.Sync()
	w.metrics.fsyncDuration.Observe(time.Since(start).Seconds())
	return err
}

// Close flushes all writes and closes active segment.
func (w *WAL) Close() (err error) {
	w.mtx.Lock()
	defer w.mtx.Unlock()

	if w.closed {
		return errors.New("wal already closed")
	}

	if w.segment == nil {
		w.closed = true
		return nil
	}

	// Flush the last page and zero out all its remaining size.
	// We must not flush an empty page as it would falsely signal
	// the segment is done if we start writing to it again after opening.
	if w.page.alloc > 0 {
		if err := w.flushPage(true); err != nil {
			return err
		}
	}

	donec := make(chan struct{})
	w.stopc <- donec
	<-donec

	if err = w.fsync(w.segment); err != nil {
		level.Error(w.logger).Log("msg", "sync previous segment", "err", err)
	}
	if err := w.segment.Close(); err != nil {
		level.Error(w.logger).Log("msg", "close previous segment", "err", err)
	}
	w.closed = true
	return nil
}

type segmentRef struct {
	name  string
	index int
}

// 列出目录下的所有segment，返回按照顺序排好的segment文件名和id
func listSegments(dir string) (refs []segmentRef, err error) {
	// 读取目录
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		fn := f.Name()
		k, err := strconv.Atoi(fn)
		if err != nil {
			continue
		}
		refs = append(refs, segmentRef{name: fn, index: k})
	}
	sort.Slice(refs, func(i, j int) bool {
		return refs[i].index < refs[j].index
	})
	for i := 0; i < len(refs)-1; i++ {
		if refs[i].index+1 != refs[i+1].index {
			return nil, errors.New("segments are not sequential")
		}
	}
	return refs, nil
}

// SegmentRange groups segments by the directory and the first and last index it includes.
type SegmentRange struct {
	Dir         string
	First, Last int
}

// NewSegmentsReader returns a new reader over all segments in the directory.
func NewSegmentsReader(dir string) (io.ReadCloser, error) {
	return NewSegmentsRangeReader(SegmentRange{dir, -1, -1})
}

// NewSegmentsRangeReader returns a new reader over the given WAL segment ranges.
// If first or last are -1, the range is open on the respective end.
func NewSegmentsRangeReader(sr ...SegmentRange) (io.ReadCloser, error) {
	var segs []*Segment

	for _, sgmRange := range sr {
		refs, err := listSegments(sgmRange.Dir)
		if err != nil {
			return nil, errors.Wrapf(err, "list segment in dir:%v", sgmRange.Dir)
		}

		for _, r := range refs {
			if sgmRange.First >= 0 && r.index < sgmRange.First {
				continue
			}
			if sgmRange.Last >= 0 && r.index > sgmRange.Last {
				break
			}
			s, err := OpenReadSegment(filepath.Join(sgmRange.Dir, r.name))
			if err != nil {
				return nil, errors.Wrapf(err, "open segment:%v in dir:%v", r.name, sgmRange.Dir)
			}
			segs = append(segs, s)
		}
	}
	return NewSegmentBufReader(segs...), nil
}

// segmentBufReader is a buffered reader that reads in multiples of pages.
// The main purpose is that we are able to track segment and offset for
// corruption reporting.  We have to be careful not to increment curr too
// early, as it is used by Reader.Err() to tell Repair which segment is corrupt.
// As such we pad the end of non-page align segments with zeros.
type segmentBufReader struct {
	buf  *bufio.Reader
	segs []*Segment
	cur  int // Index into segs.
	off  int // Offset of read data into current segment.
}

// nolint:golint // TODO: Consider exporting segmentBufReader
func NewSegmentBufReader(segs ...*Segment) *segmentBufReader {
	return &segmentBufReader{
		buf:  bufio.NewReaderSize(segs[0], 16*pageSize),
		segs: segs,
	}
}

func (r *segmentBufReader) Close() (err error) {
	for _, s := range r.segs {
		if e := s.Close(); e != nil {
			err = e
		}
	}
	return err
}

// Read implements io.Reader.
// 读取segment
func (r *segmentBufReader) Read(b []byte) (n int, err error) {
	// 先读一个字节
	n, err = r.buf.Read(b)
	r.off += n

	// If we succeeded, or hit a non-EOF, we can stop.
	if err == nil || err != io.EOF {
		return n, err
	}

	// We hit EOF; fake out zero padding at the end of short segments, so we
	// don't increment curr too early and report the wrong segment as corrupt.
	if r.off%pageSize != 0 {
		i := 0
		for ; n+i < len(b) && (r.off+i)%pageSize != 0; i++ {
			b[n+i] = 0
		}

		// Return early, even if we didn't fill b.
		r.off += i
		return n + i, nil
	}

	// There is no more deta left in the curr segment and there are no more
	// segments left.  Return EOF.
	if r.cur+1 >= len(r.segs) {
		return n, io.EOF
	}

	// Move to next segment.
	r.cur++
	r.off = 0
	r.buf.Reset(r.segs[r.cur])
	return n, nil
}

// Computing size of the WAL.
// We do this by adding the sizes of all the files under the WAL dir.
func (w *WAL) Size() (int64, error) {
	return fileutil.DirSize(w.Dir())
}
