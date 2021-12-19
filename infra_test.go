package grmq_test

import (
	"crypto/rand"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func amqpUrl(t *testing.T) string {
	require := require.New(t)

	host := envOrDefault("RMQ_HOST", "127.0.0.1")
	user := envOrDefault("RMQ_USER", "guest")
	pass := envOrDefault("RMQ_PASS", "guest")

	vhostPrefix := make([]byte, 4)
	_, err := rand.Read(vhostPrefix)
	require.NoError(err)
	vhost := fmt.Sprintf("%x_%s", vhostPrefix, strings.ToLower(t.Name()))

	vhostUrl := fmt.Sprintf("http://%s:15672/api/vhosts/%s", host, vhost)

	req, err := http.NewRequest(http.MethodPut, vhostUrl, nil)
	require.NoError(err)
	req.SetBasicAuth(user, pass)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(err)
	require.EqualValues(201, resp.StatusCode)

	t.Cleanup(func() {
		req, err := http.NewRequest(http.MethodDelete, vhostUrl, nil)
		require.NoError(err)
		req.SetBasicAuth(user, pass)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(err)
		require.EqualValues(204, resp.StatusCode)
	})

	amqpUrl := fmt.Sprintf("amqp://%s:%s@%s:5672/%s", user, pass, host, vhost)
	return amqpUrl
}

func envOrDefault(name string, defValue string) string {
	value := os.Getenv(name)
	if value != "" {
		return value
	}
	return defValue
}

func amqpChannel(t *testing.T, url string) *amqp091.Channel {
	require := require.New(t)
	c, err := amqp091.Dial(url)
	require.NoError(err)
	t.Cleanup(func() {
		err := c.Close()
		require.NoError(err)
	})

	ch, err := c.Channel()
	require.NoError(err)

	return ch
}

func declareQueue(t *testing.T, ch *amqp091.Channel, name string) {
	require := require.New(t)

	_, err := ch.QueueDeclare(name, true, false, false, false, nil)
	require.NoError(err)
}

func publishMessages(t *testing.T, ch *amqp091.Channel, queue string, count int) {
	require := require.New(t)

	for i := 0; i < count; i++ {
		err := ch.Publish("", queue, true, false, amqp091.Publishing{})
		require.NoError(err)
	}
}

func queueSize(t *testing.T, url string, queue string) int {
	require := require.New(t)

	ch := amqpChannel(t, url)

	q, err := ch.QueueInspect(queue)
	require.NoError(err)
	return q.Messages
}
