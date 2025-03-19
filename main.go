package main

import (
	"fmt"
	"time"
)

func main() {
	//Connect to database and get tokens for active users
	//Send request to TrueLayer with users and tokens
	//Push transactions to "new_transactions" kafka topic with UserID, Amount, Timestamp
	for {
		time.Sleep(1*time.Second)
		fmt.Println("Polling...")
	}
}