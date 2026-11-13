package server

import (
	"encoding/json"
	"errors"
	"os"
	"sync"
	"syscall"
	"time"

	"treenity/internal"
)

// Quanto aspettiamo che il client apra la sua FIFO in lettura.
const consumerOpenTimeout = 5 * time.Second

// Un consumer ha una CODA personale e una goroutine che la svuota
// scrivendo sulla sua FIFO. La goroutine del topic si limita ad
// accodare (enqueue), che non blocca mai: un client lento o morto
// rallenta solo la propria goroutine, non il topic né il server.
type consumer struct {
	clientID string
	topic    string
	prefix   string
	path     string
	start    uint32 // offset minimo da consegnare (--offset / segnalibro)

	mu    sync.Mutex
	queue []internal.Delivery
	fifo  *os.File

	wake     chan struct{} // "ci sono messaggi nuovi in coda"
	done     chan struct{} // chiuso da stop(): la goroutine deve uscire
	finished chan struct{} // chiuso quando la goroutine è uscita
	stopOnce sync.Once
}

// Crea la FIFO (senza aprirla: la apre la goroutine in run) e
// mette in coda i messaggi vecchi (backlog), che verranno quindi
// consegnati PRIMA di quelli nuovi.
func newConsumer(clientID, topic, prefix, path string, start uint32, backlog []internal.Delivery) (*consumer, error) {
	os.Remove(path)
	if err := syscall.Mkfifo(path, 0600); err != nil {
		return nil, err
	}
	return &consumer{
		clientID: clientID,
		topic:    topic,
		prefix:   prefix,
		path:     path,
		start:    start,
		queue:    backlog,
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
		finished: make(chan struct{}),
	}, nil
}

// Aggiunge un messaggio in coda. Non blocca mai.
func (c *consumer) enqueue(d internal.Delivery) {
	if !d.Shutdown && d.Offset < c.start {
		return // il consumer ha chiesto di partire da un offset successivo
	}
	c.mu.Lock()
	c.queue = append(c.queue, d)
	c.mu.Unlock()

	select { // sveglia la goroutine, senza bloccarsi se è già stata svegliata
	case c.wake <- struct{}{}:
	default:
	}
}

// Ferma la goroutine. Chiudere la FIFO sblocca anche una Write in corso.
func (c *consumer) stop() {
	c.stopOnce.Do(func() {
		close(c.done)
		c.mu.Lock()
		if c.fifo != nil {
			c.fifo.Close()
		}
		c.mu.Unlock()
	})
}

// Ferma la goroutine e aspetta che abbia finito (al massimo timeout),
// così la sua pulizia non cancella una FIFO nuova con lo stesso nome.
func (c *consumer) stopAndWait(timeout time.Duration) {
	c.stop()
	select {
	case <-c.finished:
	case <-time.After(timeout):
	}
}

// La goroutine del consumer. onDead viene chiamata se il client
// risulta morto (non apre la FIFO, oppure la Write fallisce con EPIPE).
func (c *consumer) run(onDead func(*consumer)) {
	defer close(c.finished)
	defer os.Remove(c.path)

	f, err := c.openWriter()
	if err != nil {
		select {
		case <-c.done: // fermato apposta: non è "morto"
		default:
			onDead(c)
		}
		return
	}
	c.mu.Lock()
	c.fifo = f
	c.mu.Unlock()
	defer f.Close()

	for {
		c.mu.Lock()
		batch := c.queue
		c.queue = nil
		c.mu.Unlock()

		for _, d := range batch {
			data, _ := json.Marshal(d)
			if _, err := f.Write(append(data, '\n')); err != nil {
				select {
				case <-c.done:
				default:
					onDead(c) // EPIPE: il client non legge più
				}
				return
			}
			if d.Shutdown {
				return
			}
		}

		select {
		case <-c.wake:
		case <-c.done:
			return
		}
	}
}

// Apre la FIFO in SOLA SCRITTURA. Con O_NONBLOCK l'apertura fallisce
// con ENXIO finché il client non l'ha aperta in lettura: riproviamo.
// Senza un lettore la Write darebbe EPIPE: è così che scopriamo i client morti.
func (c *consumer) openWriter() (*os.File, error) {
	deadline := time.Now().Add(consumerOpenTimeout)
	for {
		f, err := os.OpenFile(c.path, os.O_WRONLY|syscall.O_NONBLOCK, 0)
		if err == nil {
			return f, nil
		}
		if !errors.Is(err, syscall.ENXIO) || time.Now().After(deadline) {
			return nil, err
		}
		select {
		case <-c.done:
			return nil, errors.New("consumer stopped")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// true se qualcuno sta leggendo la FIFO del client, cioè il client è vivo.
func readerAlive(path string) bool {
	f, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return false // ENXIO (nessun lettore) o FIFO inesistente
	}
	f.Close()
	return true
}