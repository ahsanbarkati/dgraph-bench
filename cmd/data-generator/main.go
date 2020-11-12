package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/linuxerwang/dgraph-bench/tasks"
)

const (
	maxUid           = 2 * (1<<30)
	offset           = 3 * (1<<30)
	maxDirectFriends = 300
	chunkSize        = 1<<20
	k                = 100
)

var (
	output = flag.String("output", "out.rdf.gz", "Output .gz file")
)

func main() {
	flag.Parse()
	f, err := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}
	var buf strings.Builder
	w := gzip.NewWriter(f)
	s := rand.NewSource(42)
	r := rand.New(s)
	start := time.Now()

	ch := make(chan []byte, 16)
	var wg sync.WaitGroup
	wg.Add(1)
	go worker(w, ch, &wg)

	for i := 1; i <= maxUid; i++ {
		meID := fmt.Sprintf("_:n.%d", i)
		buf.WriteString(getNQuad(meID, "xid", fmt.Sprintf("\"%d\"", i+offset)))
		buf.WriteString(getNQuad(meID, "name", fmt.Sprintf("\"%s\"", tasks.RandString(10, r))))
		buf.WriteString(getNQuad(meID, "age", fmt.Sprintf("\"%d\"", 18+rand.Intn(80))))
		buf.WriteString(getNQuad(meID, "created_at", fmt.Sprintf("\"%d\"", time.Now().UnixNano())))
		buf.WriteString(getNQuad(meID, "updated_at", fmt.Sprintf("\"%d\"", time.Now().UnixNano())))

		friendCnt := randomNum()
		for j := 1; j <= friendCnt; j++ {
			fID := rand.Intn(maxUid)
			for fID == i {
				fID = rand.Intn(maxUid)
			}
			buf.WriteString(getNQuad(meID, "friend_of", fmt.Sprintf("<_:n.%d>", fID)))
		}

		if i%chunkSize == 0 {
			s := make([]byte, len(buf.String()))
			copy(s, []byte(buf.String()))
			ch <- s
			buf.Reset()
			fmt.Printf("Time per NQuad: %v, items in ch: %d\n",
				int(time.Since(start).Nanoseconds())/(i+1), len(ch))
		}

	}
	ch <- nil
	wg.Wait()
	if err := w.Flush(); err != nil {
		panic(err)
	}
	if err := w.Close(); err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	fmt.Println("Total time taken: ", time.Since(start).Seconds())
}

func randomNum() int {
	// N(t) = N0 * e^(-k*t)
	return 5 + int(maxDirectFriends*math.Exp(-k*rand.Float64()))
}

func worker(w *gzip.Writer, ch chan []byte, wg *sync.WaitGroup) {
	for buf := range ch {
		if buf == nil {
			wg.Done()
			return
		}
		if _, err := w.Write(buf); err != nil {
			panic(err)
		}
	}
}

func getNQuad(s, p, o string) string {
	return "<" + s + "> <" + p + "> " + o + " .\n"
}
