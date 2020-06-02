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

	"github.com/olekukonko/tablewriter"

	"github.com/Shopify/sarama"
)

type PemFiles struct {
	ClientCert string
	ClientKey  string
	CACert     string
}

// Default values
const (
	defaultHost  = "kafka-0.dev.ocdp-nonprod.optum.com:443" //host address
	defaultTopic = "test"                                   // topic name for testing or checking input and output

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

	tlsConfig, err := NewTLSConfig(pemFiles)
	if err != nil {
		log.Fatal(err)
	}
	// This can be used on test server if domain does not match cert:
	// tlsConfig.InsecureSkipVerify = true

	consumerConfig := sarama.NewConfig()
	consumerConfig.Net.TLS.Enable = true
	consumerConfig.Net.TLS.Config = tlsConfig

	client, err := sarama.NewClient([]string{host}, consumerConfig)
	topics, _ := client.Topics() // Listing all the topics
	fmt.Println("==============================================================")
	fmt.Println("======================Listing Topics here=====================")
	fmt.Println("==============================================================")
	time.Sleep(1 * time.Second)
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Name"})
	counter := 0
	newdata := []string{}
	for index := range topics {
		data := topics[index]
		newdata = []string{data}
		table.Append(newdata)
		counter++
	}
	table.Render()
	fmt.Println("==============================================================")
	fmt.Println("=========================List ENDS============================")
	fmt.Println("==============================================================")

	fmt.Println("Total Topics count =", counter)
	time.Sleep(1 * time.Second)

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

//=============================================================================
// Producer and Consumer Loop calling different functions
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
		case producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: getRandomValue(1200)}:

			log.Printf("Produced message %d\n", enqueued)
			enqueued++
			var wg sync.WaitGroup
			for partition := range partitions {
				wg.Add(1)
				go func() {
					consumePartition(consumer, topic, int32(partition), signals)
					time.Sleep(600 * time.Millisecond) //added producers wait
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
	fmt.Println("The total messages consumed :", enqueued)
}

//=============================================================================
// Produce random string

var letters = []rune("abcdefghijklmnopqrstuv wxyzABCDEFGH IJKL MNOPQRSTUVWXYZ 1234567890")

func getRandomValue(n int) sarama.StringEncoder {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	fmt.Println(sarama.StringEncoder(b))
	return sarama.StringEncoder(b)
}

//=============================================================================
//Consumer Behavior
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

//=============================================================================
//Consumer partition and Consumer Loop
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
	time.Sleep(700 * time.Millisecond) // added sleep to for connection timeout
ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("Consumed message offset %d\nData: %s\n", msg.Offset, msg.Value)
			fmt.Printf("Consumed message offset %d\nData: %s\n", msg.Offset, msg.Value)
			consumed++
			if consumed > 1 {
				break ConsumerLoop
			}
		case <-time.After(120 * time.Millisecond):
			break ConsumerLoop
		}
	}
	log.Printf("Consumed: %d\n", consumed)
}

