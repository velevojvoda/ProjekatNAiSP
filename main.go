package main

import (
	"ProjekatNAiSP/app/config"
	"ProjekatNAiSP/app/engine"

	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	reader := bufio.NewReader(os.Stdin)
	cfg, err := config.LoadConfig("config.json")
	if err != nil {
		fmt.Println("Error conf:", err)
		return
	}

	eng, err := engine.NewEngine(cfg)
	if err != nil {
		fmt.Println("Error conf:", err)
		return
	}
	for {
		fmt.Print("> ")
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)

		if line == "" {
			continue
		}
		if strings.ToUpper(line) == "EXIT" {
			return
		}

		parts := strings.SplitN(line, " ", 3)

		if len(parts) != 3 && len(parts) != 2 {
			fmt.Println("Format: PUT <key> <value> or GET <key> or DELETE <key>")
			continue
		}
		cmd := strings.ToUpper(parts[0])
		key := parts[1]

		switch cmd {
		case "PUT":
			if len(parts) != 3 {
				fmt.Println("Format: PUT <key> <value>")
				continue
			}
			value := parts[2]
			if err := eng.Put(key, []byte(value)); err != nil {
				fmt.Println(err)
			} else {
				fmt.Println("PUT OK")
			}
		case "GET":
			value, err := eng.Get((key))
			if err != nil {
				fmt.Println(err)
			} else if value == nil {
				fmt.Println("No key")
			} else {
				fmt.Println(value)
			}

		case "DELETE":
			if err := eng.Delete(key); err != nil {
				fmt.Println(err)
			} else {
				fmt.Println("DELETE OK")
			}
		case "EXIT":
			return
		default:
			fmt.Println("nepozanto")
		}

	}
}
