package zmqcluster

import (
	"fmt"
	"math/rand/v2"
)

func randomPort() string {
	return fmt.Sprint(5000 + rand.Int32N(2000))
}
