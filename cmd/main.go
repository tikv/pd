package main

import "fmt"

func main() {
	x := make(map[string]interface{}, 1)
	y, ok := x["hello"]
	if !ok{
		fmt.Printf("none")
	}else{
		g := y.(string)
		fmt.Printf("group=%v", g)
	}
}
