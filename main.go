package main

import (
	"crypto/rand"
	"fmt"
	mathrand "math/rand"
	"sync"
	"time"
)

type ImageStatus string

const (
	ImageStatus_Pending   = "pending"
	ImageStatus_Completed = "completed"
	ImageStatus_Error     = "error"
)

type Image struct {
	ID     string
	Status ImageStatus
}

func NewImage() *Image {
	return &Image{
		ID:     createRandId(),
		Status: ImageStatus_Pending,
	}
}

type ImageBatch struct {
	ID       string
	Images   []*Image
	Result   map[string]ImageStatus
	wg       *sync.WaitGroup
	resultCH chan *Image
}

func NewImageBatch() *ImageBatch {
	randImgCount := mathrand.Intn(10) + 5
	images := []*Image{}
	for range randImgCount {
		image := NewImage()
		images = append(images, image)
	}

	fmt.Printf("Worker count = %d\n", randImgCount)

	return &ImageBatch{
		ID:       createRandId(),
		Images:   images,
		Result:   make(map[string]ImageStatus),
		wg:       new(sync.WaitGroup),
		resultCH: make(chan *Image, randImgCount),
	}
}

func (i *Image) processImage(wg *sync.WaitGroup, resultCH chan<- *Image, id int) {
	defer wg.Done()
	randDur := mathrand.Intn(3) + 1
	sleepDur := time.Second * time.Duration(randDur)
	time.Sleep(sleepDur)
	fmt.Printf("starting workerID = %d, sleepDur = %d\n", id, randDur)

	if mathrand.Intn(10) < 5 {
		i.Status = ImageStatus_Completed
	} else {
		i.Status = ImageStatus_Error
	}
	resultCH <- i
	fmt.Printf("Finished WORKER = %d\n", id)
}

func (b *ImageBatch) process() {
	b.wg.Add(len(b.Images))
	for id, img := range b.Images {
		go img.processImage(b.wg, b.resultCH, id)
	}

	go func() {
		fmt.Println("Blocked on WG.WAIT")
		b.wg.Wait()
		close(b.resultCH)
		fmt.Println("Exiting after close of resultCH")
	}()

	for imgRes := range b.resultCH {
		b.Result[imgRes.ID] = imgRes.Status
	}
	fmt.Printf("finished processing %+v\n", b.Result)

}

func createRandId() string {
	return rand.Text()[:9]
}

// [] image batch
// [] fan-out workers(go routines) for each image
// [] fan-in results
// [] waitgroups && chans; no mutex
// [] test for race
func main() {
	batch := NewImageBatch()
	batch.process()
}
