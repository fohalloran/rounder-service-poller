package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/v5/pgxpool"
)

const clientId string = "sandbox-rounder-ff20b2"
const applicationName string = "rounder"
const dataApiUrl string = "https://api.truelayer-sandbox.com/data"
const tokenRefreshEndpoint string = "https://auth.truelayer-sandbox.com/connect/token"
const apiVersion string = "v1"

// Struct for overall response
type ApiResponse struct {
	Results []Transaction `json:"results"`
	Status  string        `json:"status"`
}

// Struct for each transaction in response
type Transaction struct {
	Timestamp   string  `json:"timestamp"`
	Description string  `json:"description"`
	Type        string  `json:"transaction_type"`
	Category    string  `json:"transaction_category"`
	Amount      float64 `json:"amount"`
	Currency    string  `json:"currency"`
}

type User struct {
	Id          int
	AccessToken string
	AccountCode string
}

type ExpiredTokenUser struct {
	Id          int
	AccountCode string
	AccessCode  string
}

type TokenResponse struct {
	AccessToken  string `json:"access_token"`
	ExpiresIn    int    `json:"expires_in"`
	TokenType    string `json:"token_type"`
	RefreshToken string `json:"refresh_token"`
	Scope        string `json:"scope"`
}

func getUsers() ([]User, error) {
	port, err := strconv.Atoi(os.Getenv("DATABASE_PORT"))
	if err != nil {
		return nil, fmt.Errorf("Invalid port number")
	}
	connection_config := pgx.ConnConfig{Host: os.Getenv("DATABASE_HOST"),
		Port:     uint16(port),
		Database: os.Getenv("DATABASE_NAME"),
		User:     os.Getenv("DATABASE_USER"),
		Password: os.Getenv("DATABASE_PASSWORD")}

	conn, err := pgx.Connect(connection_config)
	if err != nil {
		return nil, fmt.Errorf("Unable to connect to database: %v\n", err)
	}
	defer conn.Close()

	query := "SELECT id, access_token, account_code FROM \"Rounder\".users"
	rows, err := conn.Query(query)

	if err != nil {
		return nil, fmt.Errorf("Query to database failed: %v\n", err)
	}

	var users []User

	for rows.Next() {
		var newUser User
		err := rows.Scan(&newUser.Id, &newUser.AccessToken, &newUser.AccountCode)
		if err != nil {
			return nil, fmt.Errorf("Invalid data from database retrieved: %v\n", err)
		}
		users = append(users, newUser)
	}
	return users, nil
}

func getNewToken(){

}

func refreshTokens() error {
	fmt.Println("Refresh tokens started")
	
	dbURL := fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
	os.Getenv("DATABASE_USER"),
	os.Getenv("DATABASE_PASSWORD"),
	os.Getenv("DATABASE_HOST"),
	os.Getenv("DATABASE_PORT"),
	os.Getenv("DATABASE_NAME"),
	)
	fmt.Println(dbURL)

	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		return fmt.Errorf("Unable to connect to database: %v\n", err)
	}
	defer pool.Close()

	ctx := context.Background()

	query := "SELECT id, account_code, access_code FROM \"Rounder\".users WHERE token_expiry_date < now()"
	rows, err := pool.Query(ctx,query)

	if err != nil {
		return fmt.Errorf("Query to database failed: %v\n", err)
	}
	defer rows.Close()

	for rows.Next() {
		var expiredUser ExpiredTokenUser
		err := rows.Scan(&expiredUser.Id, &expiredUser.AccountCode, &expiredUser.AccessCode)
		if err != nil {
			return fmt.Errorf("Invalid data from database retrieved when getting expired tokens: %v\n", err)
		}
		data := url.Values{}

		data.Set("grant_type", "authorization_code")
		data.Set("client_id", clientId)
		data.Set("client_secret", os.Getenv("CLIENT_SECRET"))
		data.Set("redirect_uri", "https://console.truelayer.com/redirect-page")
		data.Set("code", expiredUser.AccessCode)

		payload := strings.NewReader(data.Encode())
		req, _ := http.NewRequest("POST", tokenRefreshEndpoint, payload)

		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		fmt.Println(req)
		req.Header.Add("accept", "application/x-www-form-urlencoded")
		response, err := http.DefaultClient.Do(req)
		bodyBytes, _ := io.ReadAll(response.Body)

		if err != nil {
			fmt.Println(string(bodyBytes))
			return fmt.Errorf("Error sending request to get token:", err)

		}
		if response.StatusCode != 200 {
			fmt.Println(string(bodyBytes))
			return fmt.Errorf("Error code recieved when getting token:", err)
		}
		defer response.Body.Close()
		var tokenResponse TokenResponse

		err = json.Unmarshal(bodyBytes, &tokenResponse)
		if err != nil {
			return fmt.Errorf("Error decoding JSON:", err)

		}
		expiryTime := time.Now()
		expiryTime = expiryTime.Add(time.Second * time.Duration(tokenResponse.ExpiresIn))
		expiryTimeStr := expiryTime.Format(time.RFC3339)
		query = fmt.Sprintf("UPDATE \"Rounder\".users SET access_token = '%s', access_code='%s', token_expiry_date='%s' WHERE id='%s'", tokenResponse.AccessToken, tokenResponse.RefreshToken, expiryTimeStr,strconv.Itoa(expiredUser.Id))
		fmt.Println(query)
		_, err = pool.Exec(ctx,query)
		if err != nil {
			return fmt.Errorf("Query failed:", err)
		}
	}

	return nil
}

func main() {

	err := refreshTokens()
	if err != nil {
		fmt.Println("Error refreshing tokens:", err)
		return
	}
	fmt.Println("Refresh tokens finished")
	users, err := getUsers()
	if err != nil {
		fmt.Println("Error getting users from database:", err)
		return
	}
	for _, user := range users {
		url := fmt.Sprintf("%s/%s/accounts/%s/transactions", dataApiUrl, apiVersion, user.AccountCode)
		bearer := fmt.Sprintf("Bearer %s", user.AccessToken)

		req, _ := http.NewRequest("GET", url, nil)

		req.Header.Add("accept", "application/json")
		req.Header.Add("authorization", bearer)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Println("Error sending request to get transactions:", err)
			return
		}
		if res.StatusCode != 200 {
			fmt.Println("Error code recieved when getting transactions:", res.Status)
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
			if transaction.Amount < 0 && transaction.Category == "PURCHASE" { //Filter out transactions in and non purchases TODO: see if this can be filtered in the API call to reduce data pulled
				fmt.Println(transaction)
				//Push transactions to "new_transactions" kafka topic with UserID, Amount, Timestamp

			}

		}

	}

}
