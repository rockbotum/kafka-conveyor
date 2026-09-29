package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// интерфейс консьюмера
type Consumer interface {
	Process(items []message) error //метод для обработки батча данных
}

// интерфейс продюсера
type Producer interface {
	Next(ctx context.Context, input <-chan message) (message, bool, error) //метод для получения следующего батча данных
	Commit(cookie int) error                                               //метод для подтверждения обработки батча данных
}

type message struct {
	data       any
	messageErr error
}

// структура батча
type batch struct {
	items    []message
	cookie   int
	batchErr error
}

// структура конфига
type Config struct {
	MaxBatchSize    int
	BatchTimeout    time.Duration
	MaxRetries      int
	RetryBackoff    time.Duration
	WorkerCount     int
	ShutdownTimeout time.Duration
}

// метод Pipe принимает контекст, продюсера, консьюмера и конфигурацию, и запускает процесс передачи данных от продюсера к консьюмеру с учетом заданных параметров.
func Pipe(ctx context.Context, p Producer, c Consumer, input <-chan message, cfg Config) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if p == nil || c == nil {
		cancel()
		return errors.New("invalid producer or consumer")
	}

	//параметры по умолчанию, если не заданы
	defaultConfig := Config{
		MaxBatchSize:    100,              //макс размер батча
		BatchTimeout:    5 * time.Second,  //таймаут накопления батча
		MaxRetries:      3,                //макс число попыток обработки
		RetryBackoff:    1 * time.Second,  //интервал между попытками
		WorkerCount:     5,                //количество параллельных обработчиков
		ShutdownTimeout: 10 * time.Second, //таймаут при остановке
	}

	//условие для проверки корректности конфигурации, если параметры некорректны, то используются значения по умолчанию
	if cfg.MaxBatchSize <= 0 || cfg.BatchTimeout <= 0 || cfg.MaxRetries < 0 || cfg.RetryBackoff <= 0 || cfg.WorkerCount <= 0 || cfg.ShutdownTimeout <= 0 {
		cfg = defaultConfig
	}

	batchCh := make(chan batch, cfg.WorkerCount*2)

	var wg sync.WaitGroup //WaitGroup для ожидания завершения всех горутин

	wg.Add(1) //добавления горутины в счетчик ожидания

	go func() {

		defer wg.Done() //ожидание завершения горутины

		defer close(batchCh) //закрытие канала с батчами по завершению

		oneBatch := make([]message, 0, cfg.MaxBatchSize) //создания батча как слайса сообщений с максимально доступным размером
		timer := time.NewTimer(cfg.BatchTimeout)         //задается таймер с таймаутом
		defer timer.Stop()                               //остановка таймера по завершению

		batchCookie := 0 // куки к текущему батчу

		// вспомогательная функция по обнулению буфера с батчем
		flush := func() {
			if len(oneBatch) == 0 {
				return
			}
			batchCookie++
			out := make([]message, len(oneBatch))
			copy(out, oneBatch)
			batchCh <- batch{items: out, cookie: batchCookie}
			oneBatch = oneBatch[:0]
		}

		resetTimer := func() {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(cfg.BatchTimeout)
		}

		// бесконечный икл на случаи отключения контекста, превышения время ожидания
		for {
			msg, ok, err := Next(ctx, input)
			if err != nil || !ok {
				flush()
				return
			}

			if len(oneBatch) == 0 {
				resetTimer()
			}

			oneBatch = append(oneBatch, msg)

			if len(oneBatch) >= cfg.MaxBatchSize {
				flush()
				resetTimer()
			}

			select {
			case <-timer.C:
				flush()
				resetTimer()
			default:
			}
		}
	}()

	for i := 0; i < cfg.WorkerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for item := range batchCh {
				var err error
				for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
					err = c.Process(item.items)
					if err == nil {
						break
					}
					if attempt < cfg.MaxRetries {
						time.Sleep(cfg.RetryBackoff)
					}
				}
				if err != nil {
					item.batchErr = err
					continue
				}

				for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
					err = p.Commit(item.cookie)
					if err == nil {
						break
					}
					if attempt < cfg.MaxRetries {
						time.Sleep(cfg.RetryBackoff)
					}
				}

				item.batchErr = err
			}
		}()
	}

	wg.Wait()
	return nil
}

func Process(items []message) error {
	if len(items) == 0 {
		return errors.New("no items to process")
	}

	for _, item := range items {
		if item.messageErr != nil {
			fmt.Println("Processing item:", item.data)
			fmt.Println("Processing error:", item.messageErr.Error())
		} else {
			fmt.Println("Processing item:", item.data)
		}
	}

	return nil
}

func Next(ctx context.Context, input <-chan message) (message, bool, error) {
	select {
	case <-ctx.Done():
		return message{}, false, ctx.Err()
	case msg, ok := <-input:
		if !ok {
			return message{}, false, io.EOF
		}
		return msg, true, nil
	}
}

func Commit(cookie int) error {
	fmt.Println("Committing batch:", cookie)
	return nil
}
