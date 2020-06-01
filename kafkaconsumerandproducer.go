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
	"sync"
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

	consumer, err := consumerSetup(host, pemFiles)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	producerconsumerLoop(producer, consumer, topic)

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

func producerconsumerLoop(producer sarama.AsyncProducer, consumer sarama.Consumer, topic string) {
	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Println("unable to fetch partition IDs for the topic", topic, err)
		return
	}

	var enqueued, errors int
ProducerLoop:
	for {
		select {
		case <-signals:
			break ProducerLoop
		case <-time.After(time.Millisecond * 5):
		}

		select {
		case producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: getRandomValue(10)}:
			log.Printf("Produced message %d\n", enqueued)
			fmt.Println(("Produced message %d\n", enqueued)
			enqueued++
			var wg sync.WaitGroup
			for partition := range partitions {
				wg.Add(1)
				go func() {
					consumePartition(consumer, topic, int32(partition), signals)
					wg.Done()
				}()
			}
			wg.Wait()
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
	fmt.Println("The total messages recieved is :", enqueued)
}

//=============================================================================
// Produce random string

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func getRandomValue(n int) sarama.StringEncoder {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	fmt.Println(sarama.StringEncoder(b))
	return sarama.StringEncoder(b)
}

func consumerSetup(host string, p PemFiles) (sarama.Consumer, error) {
	tlsConfig, err := NewTLSConfig(p)
	if err != nil {
		log.Fatal(err)
	}
	// This can be used on test server if domain does not match cert:
	// tlsConfig.InsecureSkipVerify = true

	consumerConfig := sarama.NewConfig()
	consumerConfig.Net.TLS.Enable = true
	consumerConfig.Net.TLS.Config = tlsConfig

	client, err := sarama.NewClient([]string{host}, consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create kafka client: %q", err)
	}

	return sarama.NewConsumerFromClient(client)
}

func consumePartition(consumer sarama.Consumer, topic string, partition int32, signals chan os.Signal) {
	log.Println("Receving on partition", partition)

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		log.Println(err)
		return
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Println(err)
		}
	}()

	consumed := 0
ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("Consumed message offset %d\nData: %s\n", msg.Offset, msg.Value)
			fmt.Println("Consumed message offset %d\nData: %s\n", msg.Offset, msg.Value)
			consumed++
			// if consumed >= 10 {
			//	break ConsumerLoop
			//}
		case <-time.After(1000 * time.Millisecond):
			break ConsumerLoop
		}
	}
	log.Printf("Consumed: %d\n", consumed)
}
