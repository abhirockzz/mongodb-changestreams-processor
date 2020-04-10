package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/abhirockzz/mongo-changestreams-go/token"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const mongoURIEnvVarName = "MONGODB_URI"
const mongoDBEnvVarName = "MONGODB_DATABASE"
const mongoCollectionEnvVarName = "MONGODB_COLLECTION"
const withResumeEnvVarName = "WITH_RESUME" //true or false (default true)

const outputfileName = "change_events"

var mongoURI string
var mongoDBName string
var mongoCollectionName string
var resumeSupported = true

func init() {
	mongoURI = os.Getenv(mongoURIEnvVarName)
	if mongoURI == "" {
		log.Fatalf("missing environment variable for %s: %s", "MongoDB URI", mongoURIEnvVarName)
	}

	mongoDBName = os.Getenv(mongoDBEnvVarName)
	if mongoDBName == "" {
		log.Fatalf("missing environment variable for %s: %s", "MongoDB DB name", mongoDBEnvVarName)
	}

	mongoCollectionName = os.Getenv(mongoCollectionEnvVarName)
	if mongoCollectionName == "" {
		log.Fatalf("missing environment variable for %s: %s", "MongoDB collection name", mongoCollectionEnvVarName)
	}

	v := os.Getenv(withResumeEnvVarName)
	if v != "" {
		var err error
		resumeSupported, err = strconv.ParseBool(v)
		if err != nil {
			log.Fatalf("Error: %v . Invalid value for environment variable: %s . Please use true or false", err, withResumeEnvVarName)
		}
	}
}

func main() {
	client, err := mongo.NewClient(options.Client().ApplyURI(mongoURI).SetDirect(true))
	if err != nil {
		log.Fatal("failed to create client: ", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	err = client.Connect(ctx)
	if err != nil {
		log.Fatal("failed to connect", err)
	}

	coll := client.Database(mongoDBName).Collection(mongoCollectionName)
	defer func() {
		err = client.Disconnect(context.Background())
		if err != nil {
			fmt.Println("failed to close connection")
		}
	}()

	matchStage := bson.D{{"$match", bson.D{{"operationType", bson.D{{"$in", bson.A{"insert", "update", "replace"}}}}}}}
	//matchStage := bson.D{{"$match", bson.D{{"operationType", "insert"}}}}
	projectStage := bson.D{{"$project", bson.M{"_id": 1, "fullDocument": 1, "ns": 1, "documentKey": 1}}}
	pipeline := mongo.Pipeline{matchStage, projectStage}
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if resumeSupported {
		t, err := token.RetrieveToken()
		if err != nil {
			log.Fatal("failed to fetch resume token: ", err)
		}
		if t != nil {
			opts.SetResumeAfter(t)
		}
	}

	op, err := os.OpenFile(outputfileName,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("failed to create output file: ", err)
	}
	defer op.Close()

	cs, err := coll.Watch(ctx, pipeline, opts)
	if err != nil {
		log.Fatal("failed to start change stream watch: ", err)
	}

	defer func() {
		close, _ := context.WithTimeout(context.Background(), 5*time.Second)
		err := cs.Close(close)
		if err != nil {
			fmt.Println("failed to close change stream")
		}
	}()

	go func() {
		fmt.Println("started change stream...")
		for cs.Next(ctx) {
			fmt.Println(cs.Current)
			re := cs.Current.Index(1) //get "fullDocument" element
			_, err := op.WriteString(re.Value().String() + "\n")
			if err != nil {
				fmt.Println("failed to save change event", err)
			}
			fmt.Printf("saved change event to file: '%s'\n", outputfileName)
		}
	}()

	exit := make(chan os.Signal)
	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

	<-exit
	fmt.Println("exit signalled. cancelling context")
	cancel()
	if resumeSupported {
		//fmt.Println("token", cs.ResumeToken().String())
		token.SaveToken(cs.ResumeToken())
	}
}
