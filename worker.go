package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Manejo de errores para que sea reutilizable en el código
func failOnError(err error, msg string) {
    if err != nil {
        log.Panicf("%s: %s", msg, err)
    }
}

// Función que realiza una solicitud POST a la API local
func post(data []byte) (*http.Response, error) {
    url := "http://localhost:8081/v1/messages" // Ajusta la URL según tu API
    resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
    if err != nil {
        log.Printf("Error enviando solicitud: %s", err)
        return nil, err
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        log.Printf("Error en la respuesta de la API: %s", resp.Status)
    }

    return resp, nil
}

func main() {
	err := godotenv.Load()
	if err != nil {
        log.Fatal("Error cargando variables de entorno: ", err)
    }
	rmqHost := os.Getenv("RMQ_HOST")
	rmqPort := os.Getenv("RMQ_PORT")
	rmqUser := os.Getenv("RMQ_USER")
	rmqPass := os.Getenv("RMQ_PASS")
	url := "amqp://"+rmqUser+":"+rmqPass+"@"+rmqHost+":"+rmqPort
    conn, err := amqp.Dial(url)
    failOnError(err, "Failed to connect to RabbitMQ")
    defer conn.Close()

    ch, err := conn.Channel()
    failOnError(err, "Failed to open a channel")
    defer ch.Close()

    q, err := ch.QueueDeclare(
        "courses", 
        true,      
        false,     
        false,    
        false,    
        nil,     
    )
    failOnError(err, "Failed to declare a queue")

    failOnError(err, "Failed to set QoS")

    msgs, err := ch.Consume(
        q.Name, 
        "",    
        true,   
        false,  
        false,  
        false,
        nil,   
    )
    failOnError(err, "Failed to register a consumer")

    var forever chan struct{}

    go func() {
        for d := range msgs {
            log.Printf("Mensaje recibido: %s", d.Body)

            var messageReceive map[string]interface{}
            err := json.Unmarshal(d.Body, &messageReceive)
            if err != nil {
                log.Printf("Error al convertir a JSON: %s", err)
                continue
            }

            message, err := json.Marshal(messageReceive)
            if err != nil {
                log.Printf("Error al convertir mensaje a JSON: %s", err)
                continue
            }

            resp, err := post(message)
			if err != nil {
                log.Printf("Error al enviar mensaje a la API: %s", err)
                continue
            }
			
            fmt.Printf("Enviado mensaje a la API: %+v\n", resp)
        }
    }()

    log.Printf(" [*] Esperando mensajes en la cola 'courses'. Para salir presiona CTRL+C")
    <-forever
}
