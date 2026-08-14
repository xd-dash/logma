package main

import (
  "fmt"
  "net/http"
  "os"
  "strconv"
  "github.com/xd-dash/logma/internal/routes"
)

func main(){
   fmt.Printf("starting server ... ")
   router := routes.NewRouter() 
   port := getPortFromArgs()
   addr := fmt.Sprintf(":%d", port)
   fmt.Printf("Go Server is listening on http://localhost%s\n", addr)
   err := http.ListenAndServe(addr, router)
   if err != nil {
     panic(err)
   }   
}

func getPortFromArgs() int {
    defaultPort := 8080
    if len(os.Args) > 1 { 
        port, err := strconv.Atoi(os.Args[1])
        if err != nil {
            fmt.Println("Invalid port provided. Using default port:", defaultPort)
            return defaultPort
        }
        return port
    }   
    return defaultPort
}
