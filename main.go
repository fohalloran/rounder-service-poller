package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

// Struct for overall response
type ApiResponse struct {
	Results []Transaction `json:"results"`
	Status  string        `json:"status"`
}

// Strcut for each transaction in response
type Transaction struct {
	Timestamp   string  `json:"timestamp"`
	Description string  `json:"description"`
	Type        string  `json:"transaction_type"`
	Category    string  `json:"transaction_category"`
	Amount      float64 `json:"amount"`
	Currency    string  `json:"currency"`
}

func main() {
	const clientId string = "sandbox-rounder-ff20b2"
	const applicationName string = "rounder"
	testBearer := "bearer " + os.Getenv("BEARER")
	//Connect to database and get tokens for active users
	url := "https://api.truelayer-sandbox.com/data/v1/accounts/56c7b029e0f8ec5a2334fb0ffc2fface/transactions"

	req, _ := http.NewRequest("GET", url, nil)

	req.Header.Add("accept", "application/json")
	req.Header.Add("authorization", testBearer)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("Error sending request:", err)
		return
	}
	if res.StatusCode != 200{
		fmt.Println("Error sending request:", res.Status)
		return
	}

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	var response ApiResponse
	err = json.Unmarshal([]byte(body), &response)
	if err != nil {
		fmt.Println("Error parsing JSON:", err)
		return
	}
	for _, transaction := range response.Results {
		if transaction.Amount < 0 && transaction.Category == "PURCHASE"{ //Filter out transactions in and non purchases TODO: see if this can be filtered in the API call
			fmt.Println(transaction)
			//Push transactions to "new_transactions" kafka topic with UserID, Amount, Timestamp

		}

	}

}
