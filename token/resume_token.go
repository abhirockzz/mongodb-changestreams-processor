package token

import (
	"fmt"
	"os"

	"go.mongodb.org/mongo-driver/bson"
)

const tokenFileName = "resume_token"

func RetrieveToken() (bson.Raw, error) {
	tf, err := os.Open(tokenFileName)
	if err != nil {
		if os.IsNotExist(err) {
			//if file does not exist
			return nil, nil
		}
		return nil, err
	}
	token, err := bson.NewFromIOReader(tf)
	if err != nil {
		//fmt.Println("failed to read token from file", err)
		return nil, err
	}
	return token, nil
}

func SaveToken(token []byte) {

	if len(token) == 0 {
		//empty token. no need to save
		return
	}
	tf, err := os.Create(tokenFileName)

	if err != nil {
		fmt.Println("token file creation failed", err)
		return
	}
	_, err = tf.Write(token)
	if err != nil {
		fmt.Println("failed to save token", err)
		return
	}
	fmt.Println("saved token to file")
}
