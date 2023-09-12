package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	clientv3 "go.etcd.io/etcd/client/v3"
	"net/http"
	lang_parser "something-proxy/lang-parser"
	"strings"
	"time"
)

type TaskAddRequest struct {
	Body string `json:"body"`
	From string `json:"from"`
}

type AddMachineRequest struct {
	MachineType string `json:"machine_type"`
	From        string `json:"from"`
}

func main() {

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		panic(err)
	}

	defer cli.Close()

	engine := gin.Default()
	engine.POST("/register", func(c *gin.Context) {
		var req AddMachineRequest
		err := c.ShouldBind(&req)
		if err != nil {
			c.JSON(400, gin.H{
				"status": "need self id",
			})
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err = cli.Put(ctx, "/machine/"+req.From, req.From)
		if err != nil {
			c.JSON(400, gin.H{
				"status": err.Error(),
			})
			return
		}
		c.Status(200)
	})
	engine.POST("/send-request", func(c *gin.Context) {
		var req TaskAddRequest
		err := c.ShouldBind(&req)
		if err != nil {
			c.JSON(400, gin.H{
				"status": "need self id",
			})
			return
		}
		input := strings.NewReader(fmt.Sprintf("%s\n", req.Body))
		read := lang_parser.NewReader(bufio.NewReader(input))
		readSexp, err := read.Read()
		//todo: calc complexity of readSexp
		fmt.Println(readSexp.String())
	})
	engine.Run(":80")

	//health check
	go func() {
		//30 sec check
		ticker := time.NewTicker(30 * time.Second)
		for {
			select {
			case <-ticker.C:
				{
					// get all machine with prefix /machine/
					res, err := cli.Get(context.Background(), "/machine/", clientv3.WithPrefix())
					if err != nil {
						panic(err)
					}
					for _, kv := range res.Kvs {
						kv := kv
						go func() {
							get, err := http.Get(string(kv.Value) + "/health")
							if err != nil || get.StatusCode != 200 {
								cli.Delete(context.Background(), "/machine/"+string(kv.Value))
								return
							}
						}()
					}
				}
			}
		}
	}()
}
