package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gofiber/fiber/v2"
	clientv3 "go.etcd.io/etcd/client/v3"
	"io/ioutil"
	"net/http"
	"something-proxy/config"
	"sort"
	"strings"
	"sync"
	"time"
)

type TaskAddRequest struct {
	Body string `json:"body"`
	From string `json:"from"`
}

type AddMachineRequestFromHeavy struct {
	MachineType string `json:"machine_type"`
	From        string `json:"from"`
}

type AddMachineRequestFromClient struct {
	MachineType       string `json:"machine_type"`
	From              string `json:"from"`
	GlobalNamespaceId string `json:"global_namespace_id"`
}

func main() {

	conf := config.Get()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{fmt.Sprintf("http://%s:%s", conf.EtcdHost, conf.EtcdPort)},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		panic(err)
	}

	defer cli.Close()

	engine := fiber.New()

	engine.Post("/register-heavy", func(c *fiber.Ctx) error {
		var req AddMachineRequestFromHeavy
		err := c.BodyParser(&req)
		if err != nil {
			return c.JSON(fiber.Map{
				"status": "need self id",
			})
		}
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
		defer cancel()

		_, err = cli.Put(ctx, "/machine-heavy/"+req.From, req.From)
		if err != nil {
			return c.JSON(fiber.Map{
				"status": err.Error(),
			})
		}
		c.Status(200)
		return nil
	})
	engine.Post("/register-client", func(c *fiber.Ctx) error {
		var req AddMachineRequestFromClient
		err := c.BodyParser(&req)
		if err != nil {
			return c.JSON(fiber.Map{
				"status": "need self id",
			})
		}
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
		defer cancel()

		_, err = cli.Put(ctx, fmt.Sprintf("/client/%s", req.From), req.From+"$"+req.GlobalNamespaceId)
		if err != nil {
			return c.JSON(fiber.Map{
				"status": err.Error(),
			})
		}
		c.Status(200)
		return nil
	})
	engine.Post("/send-request", func(c *fiber.Ctx) error {
		var req TaskAddRequest
		err := c.BodyParser(&req)
		if err != nil {
			return c.JSON(fiber.Map{
				"status": "need self id",
			})
		}
		//input := strings.NewReader(fmt.Sprintf("%s\n", req.Body))
		//read := lang_parser.NewReader(bufio.NewReader(input))
		//readSexp, err := read.Read()
		//fmt.Println(readSexp.String())
		var wg sync.WaitGroup

		// get all machine with prefix /machine/
		res, err := cli.Get(context.Background(), "/machine-heavy", clientv3.WithPrefix())
		if err != nil {
			panic(err)
		}
		var machineList []struct {
			Machine   string
			WaitCount int
		}
		for _, kv := range res.Kvs {
			kv := kv
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, err := http.Get(string(kv.Value) + "/routine-count")
				if err != nil {
					fmt.Println(err.Error())
					return
				}
				byteArray, _ := ioutil.ReadAll(resp.Body)
				var countJson struct {
					Count int `json:"count"`
				}
				json.Unmarshal(byteArray, &countJson)
				machineList = append(machineList, struct {
					Machine   string
					WaitCount int
				}{Machine: string(kv.Value), WaitCount: countJson.Count})
				defer resp.Body.Close()
			}()
		}
		wg.Wait()
		sort.Slice(machineList, func(i, j int) bool {
			return machineList[i].WaitCount < machineList[j].WaitCount
		})
		return c.JSON(fiber.Map{
			"addr": machineList[0].Machine,
		})
	})
	//health check
	go func() {
		//30 sec check
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ticker.C:
				{
					go func() {
						// get all machine with prefix /machine/
						res, err := cli.Get(context.Background(), "/machine-heavy/", clientv3.WithPrefix())
						if err != nil {
							panic(err)
						}
						for _, kv := range res.Kvs {
							kv := kv
							go func() {
								get, err := http.Get(string(kv.Value) + "/health")
								if err != nil || get.StatusCode != 200 {
									fmt.Println("delete: " + string(kv.Value))
									cli.Delete(context.Background(), "/machine-heavy/"+string(kv.Value))
									return
								}
							}()
						}
					}()
					go func() {
						// get all machine with prefix /client/
						res, err := cli.Get(context.Background(), "/client/", clientv3.WithPrefix())
						if err != nil {
							panic(err)
						}
						for _, kv := range res.Kvs {
							kv := kv
							go func() {
								fromAndNamespace := strings.Split(string(kv.Value), "$")

								get, err := http.Get(fromAndNamespace[0] + "/health")
								if err != nil || get.StatusCode != 200 {
									fmt.Println("delete: " + string(kv.Value))
									cli.Delete(context.Background(), "/env/"+fromAndNamespace[1], clientv3.WithPrefix())
									cli.Delete(context.Background(), "/client/"+fromAndNamespace[0])
									return
								}
							}()
						}
					}()
				}
			}
		}
	}()
	if err := engine.Listen(fmt.Sprintf(":%s", "80")); err != nil {
		panic(err)
	}
}
