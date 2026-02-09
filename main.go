package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
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
			fmt.Print("put")
		case "GET":
			fmt.Print("get")
		case "DELETE":
			fmt.Print("delete")
		case "EXIT":
			return
		default:
			fmt.Println("nepozanto")
		}

	}
}
