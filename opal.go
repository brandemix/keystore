/** Brandon Allen
  * May 8, 2018
  * Opal Coding Challenge
  * Interactive command-line tool for storing and retrieving key-value
  * pairs on a transactional basis.
***/

package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

var (
	store = make(map[int]map[string]string)
)

func main() {
	reader := bufio.NewReader(os.Stdin)
	transaction := 0
	store[transaction] = make(map[string]string)

	fmt.Printf("This is a simple program written in Go.\n")
	fmt.Printf("Enter h (help) for a list of commands\n")
	fmt.Printf("Ctrl-c to exit\n")

	for {
		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)

		switch true {
		case strings.HasPrefix(text, "h"):
			printCommands()
		case strings.HasPrefix(text, "SET"):
			err := setKey(text, transaction)
			if err != "" {
				fmt.Printf("=> %s\n", err)
			}
		case strings.HasPrefix(text, "GET"):
			val := getKey(text, transaction)
			fmt.Printf("=> %s\n", val)
		case strings.HasPrefix(text, "DELETE"):
			err := deleteKey(text, transaction)
			if err != "" {
				fmt.Printf("=> %s\n", err)
			}
		case strings.HasPrefix(text, "COUNT"):
			val := countKeys(text, transaction)
			fmt.Printf("=> %s\n", val)
		case strings.HasPrefix(text, "BEGIN"):
			transaction++
			initTransaction(transaction)
		case strings.HasPrefix(text, "ROLLBACK"):
			if transaction > 0 {
				delete(store, transaction)
				transaction--
			} else {
				fmt.Printf("=> no transaction\n")
			}
		case strings.HasPrefix(text, "COMMIT"):
			if transaction > 0 {
				transaction--
				commitTransaction(transaction)
			} else {
				fmt.Printf("=> no transaction\n")
			}
		}
		text = ""
	}
}

// overwrites the current transaction store with the committed transaction
// deletes the committed transaction buffer
func commitTransaction(transaction int) {
	store[transaction] = make(map[string]string)

	for key := range store[transaction+1] {
		store[transaction][key] = store[transaction+1][key]
	}

	delete(store, transaction+1)
}

// copies the previous transaction store into the new buffer
func initTransaction(transaction int) {
	store[transaction] = make(map[string]string)

	for key := range store[transaction-1] {
		store[transaction][key] = store[transaction-1][key]
	}
}

// Sets the key value pair in the store for the given transaction
// Returns an error string or empty string
func setKey(command string, transaction int) string {
	fields := strings.Fields(command)
	if len(fields) < 3 {
		return "Too few arguments - SET <key> <value>"
	}
	store[transaction][fields[1]] = fields[2]
	return ""
}

// Retrieves the value for a key for the given transaction
// Returns an error string or the value
func getKey(command string, transaction int) string {
	fields := strings.Fields(command)
	if len(fields) < 2 {
		return "Too few arguments - GET <key>"
	}
	return store[transaction][fields[1]]
}

// Deletes the map entry for a key for the given transaction
// Returns and error or empty string
func deleteKey(command string, transaction int) string {
	fields := strings.Fields(command)
	if len(fields) < 2 {
		return "Too few arguments - DELETE <key>"
	}

	if val := store[transaction][fields[1]]; val != "" {
		delete(store[transaction], fields[1])
	} else {
		return "key not set"
	}
	return ""
}

// Counts the number of times a value occurs in the store for the
// given transaction.
// Returns an error or the count
func countKeys(command string, transaction int) string {
	fields := strings.Fields(command)
	if len(fields) < 2 {
		return "Too few arguments - COUNT <value>"
	}

	count := 0
	for key := range store[transaction] {
		if store[transaction][key] == fields[1] {
			count++
		}
	}

	return strconv.Itoa(count)
}

// Displays a list of possible commands and their behavior.
func printCommands() {
	fmt.Printf("SET <key> <value> - store the value for key\n")
	fmt.Printf("GET <key>         - return the current value for key\n")
	fmt.Printf("DELETE <key>      - remove the entry for key\n")
	fmt.Printf("COUNT <value>     - return the number of keys that have the given value\n")
	fmt.Printf("BEGIN             - start a new transaction\n")
	fmt.Printf("COMMIT            - complete the current transaction\n")
	fmt.Printf("ROLLBACK          - revert to state prior to BEGIN call\n")
}
