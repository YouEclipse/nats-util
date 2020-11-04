// Copyright 2016-2019 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/bench"

	"github.com/nats-io/stan.go"
)

// Some sane defaults
const (
	DefaultNumMsgs            = 100000
	DefaultNumPubs            = 10
	DefaultNumSubs            = 0
	DefaultSync               = false
	DefaultMessageSize        = 128
	DefaultIgnoreOld          = false
	DefaultMaxPubAcksInflight = 512
	DefaultClientID           = "benchmark"
)

func usage() {
	log.Fatalf("Usage: stan-bench [-s server (%s)] [-c CLUSTER_ID] [-id CLIENT_ID] [-qgroup QUEUE_GROUP_NAME] [-np NUM_PUBLISHERS] [-ns NUM_SUBSCRIBERS] [-n NUM_MSGS] [-ms MESSAGE_SIZE] [-csv csvfile] [-mpa MAX_NUMBER_OF_PUBLISHED_ACKS_INFLIGHT] [-io] [-sync] [--creds credentials_file] [-cd PATH_TO_CERTS] [-cf CERTIFICATE_FILE] [-ck CERTIFICATE_KEY] [-u USERID] [-pw PASSWORD] <subject>\n", nats.DefaultURL)
}

var (
	benchmark  *bench.Benchmark
	qTotalRecv int32
	qSubsLeft  int32
)

func main() {
	var clusterID string
	flag.StringVar(&clusterID, "c", "test-cluster", "The NATS Streaming cluster ID")
	flag.StringVar(&clusterID, "cluster", "test-cluster", "The NATS Streaming cluster ID")

	var urls = flag.String("s", nats.DefaultURL, "The NATS server URLs (separated by comma")
	var numPubs = flag.Int("np", DefaultNumPubs, "Number of concurrent publishers")
	var numSubs = flag.Int("ns", DefaultNumSubs, "Number of concurrent subscribers")
	var numMsgs = flag.Int("n", DefaultNumMsgs, "Number of messages to publish")
	var syncPub = flag.Bool("sync", DefaultSync, "Sync message publishing")
	var messageSize = flag.Int("ms", DefaultMessageSize, "Message size in bytes.")
	var ignoreOld = flag.Bool("io", DefaultIgnoreOld, "Subscribers ignore old messages")
	var maxPubAcks = flag.Int("mpa", DefaultMaxPubAcksInflight, "Max number of published acks in flight")
	var clientID = flag.String("id", DefaultClientID, "Benchmark process base client ID")
	var csvFile = flag.String("csv", "", "Save bench data to csv file")
	var queue = flag.String("qgroup", "", "Queue group name")
	var userCreds = flag.String("creds", "", "Credentials File")
	var certDir = flag.String("cd", "", "path to certs to load")
	var certFile = flag.String("cf", "", "certificate file")
	var certKey = flag.String("ck", "", "certificate key")
	var user = flag.String("u", "", "user id")
	var pswd = flag.String("pw", "", "password")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) != 1 {
		usage()
	}

	// Setup the connect options
	opts := []nats.Option{nats.Name("NATS Streaming Benchmark")}
	// Use UserCredentials
	if *userCreds != "" {
		opts = append(opts, nats.UserCredentials(*userCreds))
	}

	// Use BasicAuth
	if len(*user) > 0 && len(*pswd) > 0 {
		opts = append(opts, nats.UserInfo(*user, *pswd))
	}

	if strings.Contains(*urls, "tls://") {
		if len(*certDir) > 0 {
			certificateFiles, _ := filepath.Glob(filepath.Join(*certDir, "*.pem"))
			opts = append(opts, nats.RootCAs(certificateFiles...))
		} else if len(*certFile) > 0 && len(*certKey) > 0 {
			opts = append(opts, nats.ClientCert(*certFile, *certKey))
		} else {
			usage()
		}
	}

	benchmark = bench.NewBenchmark("NATS Streaming", *numSubs, *numPubs)

	var startwg sync.WaitGroup
	var donewg sync.WaitGroup

	donewg.Add(*numPubs + *numSubs)

	if *queue != "" {
		qSubsLeft = int32(*numSubs)
	}
	// Run Subscribers first
	startwg.Add(*numSubs)
	for i := 0; i < *numSubs; i++ {
		subID := fmt.Sprintf("%s-sub-%d", *clientID, i)
		go runSubscriber(&startwg, &donewg, *urls, opts, clusterID, subID, *queue, *numMsgs, *messageSize, *ignoreOld)
	}
	startwg.Wait()

	// Now Publishers
	startwg.Add(*numPubs)
	pubCounts := bench.MsgsPerClient(*numMsgs, *numPubs)
	for i := 0; i < *numPubs; i++ {
		pubID := fmt.Sprintf("%s-pub-%d", *clientID, i)
		go runPublisher(&startwg, &donewg, *urls, opts, clusterID, pubCounts[i], *messageSize, *syncPub, pubID, *maxPubAcks)
	}

	log.Printf("Starting benchmark [msgs=%d, msgsize=%d, pubs=%d, subs=%d]\n", *numMsgs, *messageSize, *numPubs, *numSubs)

	startwg.Wait()
	donewg.Wait()

	benchmark.Close()
	fmt.Print(benchmark.Report())

	if len(*csvFile) > 0 {
		csv := benchmark.CSV()
		ioutil.WriteFile(*csvFile, []byte(csv), 0644)
		fmt.Printf("Saved metric data in csv file %s\n", *csvFile)
	}
}

func runPublisher(startwg, donewg *sync.WaitGroup, url string, opts []nats.Option, clusterID string, numMsgs, msgSize int, sync bool, pubID string, maxPubAcksInflight int) {
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		log.Fatalf("Publisher %s can't connect: %v\n", pubID, err)
	}
	snc, err := stan.Connect(clusterID, pubID, stan.MaxPubAcksInflight(maxPubAcksInflight), stan.NatsConn(nc),
		stan.SetConnectionLostHandler(func(_ stan.Conn, reason error) {
			log.Fatalf("Connection lost, reason: %v", reason)
		}))
	if err != nil {
		log.Fatalf("Publisher %s can't connect: %v\n", pubID, err)
	}

	startwg.Done()

	args := flag.Args()

	subj := args[0]
	var msg []byte
	if msgSize > 0 {
		//msg = make([]byte, msgSize)
		msg = []byte{10,213,14,123,34,114,101,113,117,101,115,116,95,105,100,34,58,34,56,97,54,54,57,53,55,34,44,34,114,101,113,117,101,115,116,95,109,101,116,104,111,100,34,58,34,80,79,83,84,34,44,34,114,101,113,117,101,115,116,95,117,114,108,34,58,34,104,116,116,112,115,58,47,47,97,112,105,46,105,116,101,114,97,98,108,101,46,99,111,109,47,97,112,105,47,101,118,101,110,116,115,47,116,114,97,99,107,34,44,34,111,112,101,114,97,116,105,111,110,95,116,121,112,101,34,58,34,84,114,97,99,107,69,118,101,110,116,34,44,34,117,115,101,114,95,103,108,111,98,97,108,95,105,100,34,58,34,48,100,56,57,101,53,102,99,57,99,52,56,54,97,51,97,99,52,100,53,54,101,56,55,51,52,98,56,51,57,53,99,102,101,34,44,34,116,105,109,101,95,117,110,105,120,34,58,49,54,48,52,52,56,54,52,56,57,44,34,112,114,111,112,101,114,116,105,101,115,34,58,34,123,92,34,101,118,101,110,116,78,97,109,101,92,34,58,92,34,97,99,116,105,118,105,116,121,112,97,103,101,92,34,44,92,34,99,114,101,97,116,101,100,65,116,92,34,58,49,54,48,52,52,56,54,52,56,55,57,55,50,44,92,34,100,97,116,97,70,105,101,108,100,115,92,34,58,123,92,34,97,99,116,105,118,105,116,121,73,68,92,34,58,52,53,54,48,56,44,92,34,97,99,116,105,118,105,116,121,73,109,97,103,101,92,34,58,92,34,47,97,99,116,105,118,105,116,105,101,115,47,110,117,103,121,104,115,101,101,110,119,52,112,122,121,107,122,118,102,100,117,46,106,112,103,92,34,44,92,34,97,99,116,105,118,105,116,121,78,97,109,101,92,34,58,92,34,231,141,168,229,174,182,229,132,170,230,131,160,239,188,154,233,166,153,230,184,175,229,155,155,229,173,163,233,133,146,229,186,151,228,189,143,229,174,191,229,165,151,233,164,144,92,34,44,92,34,97,99,116,105,118,105,116,121,80,114,105,99,101,92,34,58,92,34,72,75,36,50,44,53,51,48,92,34,44,92,34,98,111,111,107,105,110,103,78,117,109,98,101,114,92,34,58,49,51,54,54,44,92,34,99,97,116,101,103,111,114,121,73,68,92,34,58,51,44,92,34,99,105,116,121,73,68,92,34,58,50,44,92,34,99,105,116,121,78,97,109,101,92,34,58,92,34,233,166,153,230,184,175,92,34,44,92,34,99,105,116,121,85,82,76,92,34,58,92,34,104,116,116,112,115,58,47,47,119,119,119,46,107,108,111,111,107,46,99,111,109,47,122,104,45,72,75,47,99,105,116,121,47,50,92,34,44,92,34,99,111,117,110,116,114,121,73,68,92,34,58,50,44,92,34,112,97,103,101,85,82,76,92,34,58,92,34,104,116,116,112,115,58,47,47,119,119,119,46,107,108,111,111,107,46,99,111,109,47,122,104,45,72,75,47,97,99,116,105,118,105,116,121,47,52,53,54,48,56,92,34,44,92,34,112,108,97,116,102,111,114,109,92,34,58,92,34,65,110,100,114,111,105,100,92,34,44,92,34,114,101,99,111,109,109,101,110,100,101,100,65,99,116,105,118,105,116,121,49,92,34,58,123,92,34,73,109,97,103,101,85,82,76,92,34,58,92,34,104,116,116,112,115,58,47,47,114,101,115,46,107,108,111,111,107,46,99,111,109,47,105,109,97,103,101,47,117,112,108,111,97,100,47,97,99,116,105,118,105,116,105,101,115,47,102,57,53,98,53,101,101,102,45,65,113,117,97,76,117,110,97,45,78,105,103,104,116,45,67,114,117,105,115,101,46,106,112,103,92,34,44,92,34,78,97,109,101,92,34,58,92,34,229,188,181,228,191,157,228,187,148,232,153,159,233,171,148,233,169,151,228,185,139,230,151,133,32,40,230,140,135,229,174,154,230,153,130,229,128,153,232,178,183,51,233,128,129,49,41,92,34,44,92,34,80,114,105,99,101,92,34,58,92,34,72,75,36,50,48,49,46,48,92,34,44,92,34,98,111,111,107,105,110,103,78,117,109,98,101,114,92,34,58,49,49,56,53,49,54,44,92,34,112,97,103,101,85,82,76,92,34,58,92,34,104,116,116,112,115,58,47,47,119,119,119,46,107,108,111,111,107,46,99,111,109,47,122,104,45,72,75,47,97,99,116,105,118,105,116,121,47,54,53,57,92,34,44,92,34,114,101,118,105,101,119,78,117,109,98,101,114,92,34,58,53,57,50,57,44,92,34,114,101,118,105,101,119,82,97,116,105,110,103,92,34,58,52,46,55,125,44,92,34,114,101,99,111,109,109,101,110,100,101,100,65,99,116,105,118,105,116,121,50,92,34,58,123,92,34,73,109,97,103,101,85,82,76,92,34,58,92,34,104,116,116,112,115,58,47,47,114,101,115,46,107,108,111,111,107,46,99,111,109,47,105,109,97,103,101,47,117,112,108,111,97,100,47,97,99,116,105,118,105,116,105,101,115,47,111,56,120,106,107,49,113,97,121,103,114,109,117,115,119,108,103,118,103,107,46,106,112,103,92,34,44,92,34,78,97,109,101,92,34,58,92,34,227,128,144,229,141,179,232,178,183,229,141,179,231,148,168,227,128,145,233,166,153,230,184,175,230,169,159,229,160,180,229,191,171,231,183,154,232,187,138,231,165,168,239,188,136,230,142,131,81,82,32,67,111,100,101,231,155,180,230,142,165,229,133,165,233,150,152,239,188,137,92,34,44,92,34,80,114,105,99,101,92,34,58,92,34,72,75,36,52,57,46,48,92,34,44,92,34,98,111,111,107,105,110,103,78,117,109,98,101,114,92,34,58,52,48,50,57,48,54,56,44,92,34,112,97,103,101,85,82,76,92,34,58,92,34,104,116,116,112,115,58,47,47,119,119,119,46,107,108,111,111,107,46,99,111,109,47,122,104,45,72,75,47,97,99,116,105,118,105,116,121,47,55,49,92,34,44,92,34,114,101,118,105,101,119,78,117,109,98,101,114,92,34,58,50,57,51,56,50,52,44,92,34,114,101,118,105,101,119,82,97,116,105,110,103,92,34,58,52,46,57,125,44,92,34,114,101,99,111,109,109,101,110,100,101,100,65,99,116,105,118,105,116,121,51,92,34,58,123,92,34,73,109,97,103,101,85,82,76,92,34,58,92,34,104,116,116,112,115,58,47,47,114,101,115,46,107,108,111,111,107,46,99,111,109,47,105,109,97,103,101,47,117,112,108,111,97,100,47,97,99,116,105,118,105,116,105,101,115,47,119,103,119,55,102,56,110,48,110,108,122,106,48,54,51,113,122,121,107,52,46,106,112,103,92,34,44,92,34,78,97,109,101,92,34,58,92,34,227,128,144,231,141,168,229,174,182,229,132,170,230,131,160,227,128,145,233,166,153,230,184,175,232,191,170,229,163,171,229,176,188,230,168,130,229,156,146,233,150,128,231,165,168,32,43,32,229,146,150,229,149,161,229,132,170,230,131,160,92,34,44,92,34,80,114,105,99,101,92,34,58,92,34,72,75,36,53,55,51,46,48,92,34,44,92,34,98,111,111,107,105,110,103,78,117,109,98,101,114,92,34,58,50,55,53,53,52,57,56,44,92,34,112,97,103,101,85,82,76,92,34,58,92,34,104,116,116,112,115,58,47,47,119,119,119,46,107,108,111,111,107,46,99,111,109,47,122,104,45,72,75,47,97,99,116,105,118,105,116,121,47,51,57,92,34,44,92,34,114,101,118,105,101,119,78,117,109,98,101,114,92,34,58,49,50,56,52,54,57,44,92,34,114,101,118,105,101,119,82,97,116,105,110,103,92,34,58,52,46,56,125,44,92,34,114,101,118,105,101,119,78,117,109,98,101,114,92,34,58,49,57,50,44,92,34,114,101,118,105,101,119,82,97,116,105,110,103,92,34,58,52,46,55,44,92,34,118,101,114,116,105,99,97,108,84,121,112,101,92,34,58,92,34,65,99,116,105,118,105,116,105,101,115,32,92,92,117,48,48,50,54,32,69,120,112,101,114,105,101,110,99,101,115,92,34,125,44,92,34,117,115,101,114,73,100,92,34,58,92,34,48,100,56,57,101,53,102,99,57,99,52,56,54,97,51,97,99,52,100,53,54,101,56,55,51,52,98,56,51,57,53,99,102,101,92,34,125,34,125,18,30,10,8,75,77,81,45,84,101,115,116,18,8,75,77,81,45,84,101,115,116,40,136,189,254,221,252,139,146,162,22}
	}
	fmt.Println(len(msg))
	published:=0
	start := time.Now()

	if !sync {
		ch := make(chan bool)
		acb := func(lguid string, err error) {
			if err != nil {
				log.Fatalf("Publisher %q got following error: %v", pubID, err)
			}
			published++
			if published >= numMsgs {
				ch <- true
			}
		}
		for i := 0; i < numMsgs; i++ {
			_, err := snc.PublishAsync(subj, msg, acb)
			if err != nil {
				log.Fatal(err)
			}
		}
		<-ch
	} else {
		for i := 0; i < numMsgs; i++ {
			err := snc.Publish(subj, msg)
			if err != nil {
				log.Fatal(err)
			}
			published++
		}
	}

	benchmark.AddPubSample(bench.NewSample(numMsgs, msgSize, start, time.Now(), snc.NatsConn()))
	snc.Close()
	nc.Close()
	donewg.Done()
}

func runSubscriber(startwg, donewg *sync.WaitGroup, url string, opts []nats.Option, clusterID, subID, queue string, numMsgs, msgSize int, ignoreOld bool) {
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		log.Fatalf("Subscriber %s can't connect: %v\n", subID, err)
	}
	snc, err := stan.Connect(clusterID, subID, stan.NatsConn(nc),
		stan.SetConnectionLostHandler(func(_ stan.Conn, reason error) {
			log.Fatalf("Connection lost, reason: %v", reason)
		}))
	if err != nil {
		log.Fatalf("Subscriber %s can't connect: %v\n", subID, err)
	}

	args := flag.Args()
	subj := args[0]
	ch := make(chan time.Time, 2)

	isQueue := queue != ""
	received := 0
	mcb := func(msg *stan.Msg) {
		received++
		if received == 1 {
			ch <- time.Now()
		}
		if isQueue {
			if atomic.AddInt32(&qTotalRecv, 1) >= int32(numMsgs) {
				ch <- time.Now()
			}
		} else {
			if received >= numMsgs {
				ch <- time.Now()
			}
		}
	}

	var sub stan.Subscription
	if ignoreOld {
		sub, err = snc.QueueSubscribe(subj, queue, mcb)
	} else {
		sub, err = snc.QueueSubscribe(subj, queue, mcb, stan.DeliverAllAvailable())
	}
	if err != nil {
		log.Fatalf("Subscriber %s can't subscribe: %v", subID, err)
	}
	startwg.Done()

	start := <-ch
	end := <-ch
	benchmark.AddSubSample(bench.NewSample(received, msgSize, start, end, snc.NatsConn()))
	// For queues, since not each member receives the total number of messages,
	// when a member is done, it needs to publish a message to unblock other member(s).
	if isQueue {
		if sr := atomic.AddInt32(&qSubsLeft, -1); sr > 0 {
			// Close this queue member first so that there is no chance that the
			// server sends the message we are going to publish back to this member.
			sub.Close()
			snc.Publish(subj, []byte("done"))
		}
	}
	snc.Close()
	nc.Close()
	donewg.Done()
}
