package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"syscall"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/gorilla/mux"
	"github.com/minio/cli"
	"github.com/ncw/directio"
)

func main() {
	app := cli.NewApp()
	app.Usage = "HTTP throughput benchmark"
	app.Commands = []cli.Command{
		{
			Name:   "client",
			Usage:  "run client",
			Action: runClient,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "server",
					Usage: "http://server:port",
					Value: "",
				},
				cli.StringFlag{
					Name:  "duration",
					Usage: "duration",
					Value: "10",
				},
				cli.StringFlag{
					Name:  "size",
					Usage: "size",
					Value: "128MiB",
				},
				cli.IntFlag{
					Name:  "scale",
					Usage: "scale",
					Value: 1,
				},
				cli.BoolFlag{
					Name:  "directio",
					Usage: "bypass kernel cache for writes and reads",
				},
			},
		},
		{
			Name:   "server",
			Usage:  "run server",
			Action: runServer,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "port",
					Usage: "port",
					Value: "8000",
				},
				cli.BoolFlag{
					Name:  "devnull",
					Usage: "data not written/read to/from disks",
				},
				cli.BoolFlag{
					Name:  "directio",
					Usage: "bypass kernel cache for writes and reads",
				},
			},
		},
	}
	app.RunAndExitOnError()
}

func runServer(ctx *cli.Context) {
	port := ctx.String("port")
	devnull := ctx.Bool("devnull")
	dio := ctx.Bool("directio")

	blkSize := 4 * 1024 * 1024
	router := mux.NewRouter()
	router.Methods(http.MethodPut).HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		filePath := r.URL.Path
		writer := ioutil.Discard
		if !devnull {
			var flag int
			flag = os.O_CREATE | os.O_WRONLY
			if dio {
				flag = flag | syscall.O_DIRECT
			}
			f, err := os.OpenFile(filePath, flag, 0644)
			if err != nil {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(err.Error()))
				return
			}
			writer = f
			defer f.Close()
		}
		b := directio.AlignedBlock(blkSize)
		_, err := io.CopyBuffer(writer, r.Body, b)
		if err != nil {
			log.Fatal(err)
		}
	})
	router.Methods(http.MethodGet).HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		filePath := r.URL.Path
		var reader io.Reader
		reader = &clientReader{0, make(chan struct{})}
		if !devnull {
			var flag int
			flag = os.O_RDONLY
			if dio {
				flag = flag | syscall.O_DIRECT
			}
			f, err := os.OpenFile(filePath, flag, 0644)
			if err != nil {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(err.Error()))
				return
			}
			defer f.Close()
			reader = f
		}
		b := directio.AlignedBlock(blkSize)
		io.CopyBuffer(w, reader, b)
	})
	router.Methods(http.MethodDelete).HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !devnull {
			os.Remove(r.URL.Path)
		}
	})
	log.Fatal(http.ListenAndServe(":"+port, router))
}

type clientReader struct {
	n      int
	doneCh chan struct{}
}

func (c *clientReader) Read(b []byte) (n int, err error) {
	select {
	case <-c.doneCh:
		return 0, io.EOF
	default:
		c.n += len(b)
		return len(b), nil
	}
}

func runClient(ctx *cli.Context) {
	server := ctx.String("server")
	scale := ctx.Int("scale")
	sizeStr := ctx.String("size")
	size, err := humanize.ParseBytes(sizeStr)
	if err != nil {
		log.Fatal(err)
	}

	dio := ctx.Bool("directio")
	if dio && server != "" {
		fmt.Println(`for directio on the server side, --directio needs to be passed to "througput server" command`)
	}

	files := ctx.Args()
	if len(files) == 0 {
		cli.ShowCommandHelpAndExit(ctx, "", 1)
	}
	doneCh := make(chan struct{})
	t1 := time.Now()
	var wg sync.WaitGroup
	for _, file := range files {
		for i := 0; i < scale; i++ {
			wg.Add(1)
			go func(file string, i int) {
				defer wg.Done()
				r := &clientReader{0, doneCh}
				req, err := http.NewRequest(http.MethodPost, server+"/minio/storage/v6"+file+"/createfile", io.LimitReader(r, int64(size)))
				if err != nil {
					log.Fatal(err)
				}
				req.URL.RawQuery = fmt.Sprintf("volume=testbucket&file-path=tmpfile%d&length=%d", i, size)
				_, err = http.DefaultClient.Do(req)
				if err != nil {
					log.Fatal(err)
				}
			}(file, i)
		}
	}
	wg.Wait()
	totalWritten := int(size) * len(files) * scale
	t2 := time.Now()
	timeD := int(t2.Sub(t1).Seconds())
	fmt.Println("Write speed: ", humanize.Bytes(uint64(totalWritten/timeD)))
	fmt.Println("Write speed per drive: ", humanize.Bytes(uint64(totalWritten/len(files)*scale/timeD)))
	// doneCh = make(chan struct{})
	// for _, file := range files {
	// 	go func(file string) {
	// 		b := directio.AlignedBlock(blkSize)
	// 		totalRead := 0
	// 		for {
	// 			if server == "" {
	// 				var flag int
	// 				flag = os.O_RDONLY
	// 				if dio {
	// 					flag = flag | syscall.O_DIRECT
	// 				}
	// 				f, err := os.OpenFile(file, flag, 0644)
	// 				if err != nil {
	// 					log.Fatal(err)
	// 				}

	// 				for {
	// 					select {
	// 					case <-doneCh:
	// 						transferCh <- totalRead
	// 						f.Close()
	// 						return
	// 					default:
	// 					}
	// 					n, err := f.Read(b)
	// 					totalRead += n
	// 					if err == io.EOF {
	// 						f.Close()
	// 						break
	// 					}
	// 					if err != nil {
	// 						log.Fatal(err)
	// 					}
	// 				}
	// 			} else {
	// 				resp, err := http.Get(server + file)
	// 				if err != nil {
	// 					log.Fatal(err)
	// 				}
	// 				if resp.StatusCode != http.StatusOK {
	// 					log.Fatal(err)
	// 				}
	// 				for {
	// 					select {
	// 					case <-doneCh:
	// 						resp.Body.Close()
	// 						transferCh <- totalRead
	// 						return
	// 					default:
	// 					}
	// 					n, err := resp.Body.Read(b)
	// 					totalRead += n
	// 					if err == io.EOF {
	// 						resp.Body.Close()
	// 						break
	// 					}
	// 					if err != nil {
	// 						log.Fatal(err)
	// 					}
	// 				}
	// 			}
	// 		}
	// 	}(file)
	// }
	// time.Sleep(time.Duration(duration) * time.Second)
	// close(doneCh)
	// totalRead := 0
	// for _ = range files {
	// 	n := <-transferCh
	// 	totalRead += n
	// }
	// fmt.Println("Read speed: ", humanize.Bytes(uint64(totalRead/duration)))
	// for _, file := range files {
	// 	if server == "" {
	// 		os.Remove(file)
	// 	} else {
	// 		req, err := http.NewRequest(http.MethodDelete, server+file, nil)
	// 		if err != nil {
	// 			log.Fatal(err)
	// 		}
	// 		resp, err := http.DefaultClient.Do(req)
	// 		if err != nil {
	// 			log.Fatal(err)
	// 		}
	// 		if resp.StatusCode != http.StatusOK {
	// 			log.Fatal(err)
	// 		}
	// 	}
	// }
}
