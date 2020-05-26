package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
)

type PemFiles struct {
	ClientCert string
	ClientKey  string
	CACert     string
}

// Default values
const (
	defaultHost  = "kafka-0.stage.ocdp-nonprod.optum.com:443"
	defaultTopic = "test"

	defaultClientCert = "client.cer.pem"
	defaultClientKey  = "client.key.pem"
	defaultCACert     = "server.cer.pem"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	var (
		host       string
		topic      string
		clientCert string
		clientKey  string
		caCert     string
	)

	flag.StringVar(&host, "host", defaultHost, "Kafka host")
	flag.StringVar(&topic, "topic", defaultTopic, "Kafka topic")
	flag.StringVar(&clientCert, "clientCert", defaultClientCert, "Client Certificate")
	flag.StringVar(&clientKey, "clientKey", defaultClientKey, "Client Key")
	flag.StringVar(&caCert, "caCert", defaultCACert, "CA Certificate")
	flag.Parse()

	pemFiles := PemFiles{
		ClientCert: clientCert,
		ClientKey:  clientKey,
		CACert:     caCert,
	}

	producer, err := producerSetup(host, pemFiles)
	if err != nil {
		log.Fatal(err)
	}

	producerLoop(producer, topic)

	if err := producer.Close(); err != nil {
		log.Fatalln(err)
	}
}

//------------------------------------
// function auth

func NewTLSConfig(pemFiles PemFiles) (*tls.Config, error) {
	tlsConfig := tls.Config{}

	// Load client cert
	cert, err := tls.LoadX509KeyPair(pemFiles.ClientCert, pemFiles.ClientKey)
	if err != nil {
		return &tlsConfig, err
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	// Load CA cert
	caCert, err := ioutil.ReadFile(pemFiles.CACert)
	if err != nil {
		return &tlsConfig, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool

	tlsConfig.BuildNameToCertificate()
	return &tlsConfig, err
}

//=============================================================================
// Producer behaviour
func producerSetup(host string, p PemFiles) (sarama.AsyncProducer, error) {
	tlsConfig, err := NewTLSConfig(p)
	if err != nil {
		log.Fatal(err)
	}
	// This can be used on test server if domain does not match cert:
	// tlsConfig.InsecureSkipVerify = true

	producerConfig := sarama.NewConfig()
	producerConfig.Net.TLS.Enable = true
	producerConfig.Net.TLS.Config = tlsConfig

	client, err := sarama.NewClient([]string{host}, producerConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create kafka client: %q", err)
	}

	return sarama.NewAsyncProducerFromClient(client)
}

func producerLoop(producer sarama.AsyncProducer, topic string) {
	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var enqueued, errors int
ProducerLoop:
	for {
		select {
		case <-signals:
			break ProducerLoop
		case <-time.After(time.Second * 1):
		}

		select {
		case producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: getRandomValue(10)}:
			log.Printf("Produced message %d\n", enqueued)
			enqueued++
			if enqueued >= 10 { // added this to limit it to 10 enteries
				goto LOOPEND
			}
		case err := <-producer.Errors():
			log.Println("Failed to produce message", err)
			errors++
		}
	}
	log.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
LOOPEND: // defined loop end might not work
}

//=============================================================================
// Produce random string

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func getRandomValue(n int) sarama.StringEncoder {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return sarama.StringEncoder(b)
}
