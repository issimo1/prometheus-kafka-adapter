// Copyright 2018 Telefónica
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/contrib/ginrus"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

func main() {
	logrus.Info("creating kafka producer")

	kafkaConfig := kafka.ConfigMap{
		/*
		   {_RK_GLOBAL | _RK_PRODUCER | _RK_HIGH, "queue.buffering.max.messages",
		    _RK_C_INT, _RK(queue_buffering_max_msgs),
		    "Maximum number of messages allowed on the producer queue. "
		    "This queue is shared by all topics and partitions. A value of 0 disables "
		    "this limit.",
		    0, INT_MAX, 100000},
		*/
		// 三层结构
		// prometheus-kafka-adapter
		// 			 v
		// 		kafka-go-client
		//			 v
		//	     librdkafka (queue.buffering.max.messages)
		// 1,000,000 时大约占用1.17GB 单个消息占用1117Bytes 大约是1.2KB
		"go.produce.channel.size":      kafkaProduceChannelSize,
		"queue.buffering.max.messages": kafkaBufferQueueSize,
		"bootstrap.servers":            kafkaBrokerList,
		"compression.codec":            kafkaCompression,
		"batch.num.messages":           kafkaBatchNumMessages,
		"go.batch.producer":            kafkaBatchProduce, // Enable batch producer (for increased performance).
		"go.delivery.reports":          false,             // per-message delivery reports to the Events() channel
		"acks":                         kafkaAcks,
	}

	if kafkaSslClientCertFile != "" && kafkaSslClientKeyFile != "" && kafkaSslCACertFile != "" {
		if kafkaSecurityProtocol == "" {
			kafkaSecurityProtocol = "ssl"
		}

		if kafkaSecurityProtocol != "ssl" && kafkaSecurityProtocol != "sasl_ssl" {
			logrus.Fatal("invalid config: kafka security protocol is not ssl based but ssl config is provided")
		}

		kafkaConfig["security.protocol"] = kafkaSecurityProtocol
		kafkaConfig["ssl.ca.location"] = kafkaSslCACertFile              // CA certificate file for verifying the broker's certificate.
		kafkaConfig["ssl.certificate.location"] = kafkaSslClientCertFile // Client's certificate
		kafkaConfig["ssl.key.location"] = kafkaSslClientKeyFile          // Client's key
		kafkaConfig["ssl.key.password"] = kafkaSslClientKeyPass          // Key password, if any.
	}

	if kafkaSaslMechanism != "" && kafkaSaslUsername != "" && kafkaSaslPassword != "" {
		if kafkaSecurityProtocol != "sasl_ssl" && kafkaSecurityProtocol != "sasl_plaintext" {
			logrus.Fatal("invalid config: kafka security protocol is not sasl based but sasl config is provided")
		}

		kafkaConfig["security.protocol"] = kafkaSecurityProtocol
		kafkaConfig["sasl.mechanism"] = kafkaSaslMechanism
		kafkaConfig["sasl.username"] = kafkaSaslUsername
		kafkaConfig["sasl.password"] = kafkaSaslPassword

		if kafkaSslCACertFile != "" {
			kafkaConfig["ssl.ca.location"] = kafkaSslCACertFile
		}
	}

	producer, err := kafka.NewProducer(&kafkaConfig)

	if err != nil {
		logrus.WithError(err).Fatal("couldn't create kafka producer")
	}

	r := gin.New()

	r.Use(ginrus.Ginrus(logrus.StandardLogger(), time.RFC3339, true), gin.Recovery())

	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
	r.GET("/healthz", func(c *gin.Context) { c.JSON(200, gin.H{"status": "UP"}) })
	if basicauth {
		authorized := r.Group("/", gin.BasicAuth(gin.Accounts{
			basicauthUsername: basicauthPassword,
		}))
		authorized.POST("/receive", receiveHandler(producer, serializer))
	} else {
		r.POST("/receive", receiveHandler(producer, serializer))
	}

	logrus.Fatal(r.Run())
}
