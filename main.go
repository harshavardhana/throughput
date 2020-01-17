package main

import (
	"errors"
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
				cli.StringFlag{
					Name:  "block-size",
					Value: "4MiB",
				},
			},
		},
	}
	app.RunAndExitOnError()
}

func runServer(ctx *cli.Context) {
	port := ctx.String("port")
	blkSize, err := humanize.ParseBytes(ctx.String("block-size"))
	if err != nil {
		log.Fatal(err)
	}

	devnull := ctx.Bool("devnull")
	dio := ctx.Bool("directio")

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
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			writer = f
			defer f.Close()
		}
		b := directio.AlignedBlock(int(blkSize))
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
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			defer f.Close()
			reader = f
		}
		b := directio.AlignedBlock(int(blkSize))
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
		wg.Add(1)
		go func(file string) {
			defer wg.Done()
			r := &clientReader{0, doneCh}
			req, err := http.NewRequest(http.MethodPut, server+file, io.LimitReader(r, int64(size)))
			if err != nil {
				log.Fatal(err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Fatal(err)
			}
			if resp.StatusCode != http.StatusOK {
				log.Fatal(errors.New(resp.Status))
			}
		}(file)
	}
	wg.Wait()
	totalWritten := int(size) * len(files)
	t2 := time.Now()

	fmt.Println("Write speed: ", humanize.IBytes(uint64(float64(totalWritten)/t2.Sub(t1).Seconds())))
	fmt.Println("Write speed per drive: ", humanize.IBytes(uint64(float64(totalWritten)/float64(len(files))/t2.Sub(t1).Seconds())))
	t1 = time.Now()
	wg = sync.WaitGroup{}
	for _, file := range files {
		wg.Add(1)
		go func(file string) {
			defer wg.Done()
			resp, err := http.Get(server + file)
			if err != nil {
				log.Fatal(err)
			}
			if resp.StatusCode != http.StatusOK {
				log.Fatal(err)
			}
			io.Copy(ioutil.Discard, resp.Body)
		}(file)
	}
	wg.Wait()

	totalRead := int(size) * len(files)
	t2 = time.Now()

	fmt.Println("Read speed: ", humanize.IBytes(uint64(float64(totalRead)/t2.Sub(t1).Seconds())))
	fmt.Println("Read speed per drive: ", humanize.IBytes(uint64(float64(totalRead)/float64(len(files))/t2.Sub(t1).Seconds())))
}
