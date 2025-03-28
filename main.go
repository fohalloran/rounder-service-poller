package main

import (
	"context"
	"crypto/sha256"
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
	"github.com/redis/go-redis/v9"
)

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
	RefreshToken string
	AccountCode  string
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
		Password: os.Getenv("DATABASE_POSTGRES_PASSWORD")}

	conn, err := pgx.Connect(connection_config)
	if err != nil {
		return nil, fmt.Errorf("Unable to connect to database: %w\n", err)
	}
	defer conn.Close()

	query := "SELECT account_code, refresh_token FROM \"Rounder\".users"
	rows, err := conn.Query(query)

	if err != nil {
		return nil, fmt.Errorf("Query to database failed: %w\n", err)
	}

	var users []User

	for rows.Next() {
		var newUser User
		err := rows.Scan(&newUser.AccountCode, &newUser.RefreshToken)
		if err != nil {
			return nil, fmt.Errorf("Invalid data from database retrieved: %w\n", err)
		}
		users = append(users, newUser)
	}
	return users, nil
}

func refreshBearerToken(user User, rdb *redis.Client, ctx context.Context) error {
	fmt.Printf("Refreshing token for AccountCode: %s\n", user.AccountCode)
	requestData := url.Values{}

	requestData.Set("grant_type", "refresh_token")
	requestData.Set("client_id", os.Getenv("TL_CLIENT_ID"))
	requestData.Set("client_secret", os.Getenv("CLIENT_SECRET"))
	requestData.Set("refresh_token", user.RefreshToken)

	payload := strings.NewReader(requestData.Encode())
	req, err := http.NewRequest("POST", tokenRefreshEndpoint, payload)
	if err != nil {
		return fmt.Errorf("Error generating post request for token refresh: %w", err)
	}

	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("accept", "application/x-www-form-urlencoded")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error sending POST request for token refresh: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("Error code recieved when getting refresh token:", response.StatusCode)
	}

	bodyBytes, err := io.ReadAll(response.Body)
	defer response.Body.Close()
	if err != nil {
		return fmt.Errorf("error reading response body of refresh token: %w", err)
	}

	var tokenResponse TokenResponse

	err = json.Unmarshal(bodyBytes, &tokenResponse)
	if err != nil {
		return fmt.Errorf("Error decoding JSON:", err)

	}
	expiryTime := time.Duration(tokenResponse.ExpiresIn) * time.Second
	hashedKey := sha256.Sum256([]byte(user.AccountCode))
	err = rdb.Set(ctx, string(hashedKey[:]), tokenResponse.AccessToken, expiryTime).Err()
	if err != nil {
		return fmt.Errorf("Error setting new bearer token :", err)
	}
	return nil
}

func getBearerToken(user User, rdb *redis.Client) (string, error) {
	var bearerToken string
	ctx := context.Background()
	hashedKeyBytes := sha256.Sum256([]byte(user.AccountCode))
	hashedKey := string(hashedKeyBytes[:])
	bearerToken, err := rdb.Get(ctx, hashedKey).Result()
	if err == redis.Nil { // Bearer token has expired
		err = refreshBearerToken(user, rdb, ctx)
		if err != nil {
			return "", fmt.Errorf("Error refreshing token for '%s': %w", user.AccountCode, err)
		}
		bearerToken, err = rdb.Get(ctx, hashedKey).Result()
	} else if err != nil {
		return "", fmt.Errorf("Error getting bearerToken for '%s': %w", user.AccountCode, err)
	}

	return bearerToken, nil
}

func getTransactions(user User, rdb *redis.Client) ([]Transaction, error) {
	bearerToken, err := getBearerToken(user, rdb)
	if err != nil {
		return nil, fmt.Errorf("Error getting bearer token for user '%s': %w", user.AccountCode, err)
	}
	url := fmt.Sprintf("%s/%s/accounts/%s/transactions", dataApiUrl, apiVersion, user.AccountCode)
	bearer := fmt.Sprintf("Bearer %s", bearerToken)

	req, _ := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("Error generating GET request for transaction data : %w", err)
	}

	req.Header.Add("accept", "application/json")
	req.Header.Add("authorization", bearer)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Error sending request to get transactions:", err)

	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Error code recieved when getting transactions:", res.Status)

	}

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	var response ApiResponse
	var transactions []Transaction
	err = json.Unmarshal([]byte(body), &response)
	if err != nil {
		return nil, fmt.Errorf("Error parsing JSON:", err)
	}
	for _, transaction := range response.Results {
		if transaction.Amount < 0 && transaction.Category == "PURCHASE" { //Filter out transactions in and non purchases TODO: see if this can be filtered in the API call to reduce data pulled
			transactions = append(transactions, transaction)

		}

	}
	return transactions, nil

}

func pushTransactions(transactions []Transaction) error {
	return nil
}

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("DATABASE_REDIS_ADDRESS"),
		Username: os.Getenv("DATABASE_REDIS_USER"),
		Password: os.Getenv("DATABASE_REDIS_PASSWORD"),
		DB:       0, // Use default DB
	})

	users, err := getUsers()
	if err != nil {
		fmt.Println("Error getting users from database:", err)
		return
	}
	for _, user := range users {
		transactions, err := getTransactions(user, rdb)
		if err != nil {
			fmt.Println("Error getting transactions data:", err)
			continue
		}
		fmt.Println(transactions)
		err = pushTransactions(transactions)
		if err != nil {
			fmt.Println("Error pushing transactions to kafka:", err)
			continue
		}
	}

}
