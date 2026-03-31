package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"ProjekatNAiSP/app/config"
	"ProjekatNAiSP/app/engine"
)

func main() {
	cfg, err := config.LoadConfig("config.json")

	if err != nil {
		fmt.Println("Error conf:", err)
		return
	}

	eng, err := engine.NewEngine(cfg)
	if err != nil {
		fmt.Println("Error engine:", err)
		return
	}

	if err := eng.Recover(); err != nil {
		fmt.Println("Error recovery:", err)
		return
	}

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 3)
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "PUT":
			if len(parts) < 3 {
				fmt.Println("Format: PUT <key> <value> or GET <key> or DELETE <key>")
				continue
			}
			key := parts[1]
			value := parts[2]

			if err := eng.Put(key, []byte(value)); err != nil {
				fmt.Println("Error:", err)
			} else {
				fmt.Println("OK")
			}

		case "GET":
			if len(parts) < 2 {
				fmt.Println("Format: PUT <key> <value>")
				continue
			}
			key := parts[1]

			value, err := eng.Get(key)
			if err != nil {
				fmt.Println("Error:", err)
			} else if value == nil {
				fmt.Println("There is no such key")
			} else {
<<<<<<< HEAD
				fmt.Println(string(value))
=======
				fmt.Println(string((value)))
>>>>>>> nina
			}

		case "DELETE":
			if len(parts) < 2 {
				fmt.Println("Format: DELETE <key>")
				continue
			}
			key := parts[1]

			if err := eng.Delete(key); err != nil {
				fmt.Println("Error:", err)
			} else {
				fmt.Println("OK")
			}

		case "EXIT":
			eng.Shutdown()
			return

		default:
			fmt.Println("Unknown command. Format: PUT <key> <value> or GET <key> or DELETE <key>")
		}
	}
}
