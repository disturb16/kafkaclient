# kafkaclient
Golang kafka package to simplify te consumer and producer usage

## Install
go get github.com/disturb16/kafkaclient

## Consumer
```
func onMessageReceived(topic string, message string){
  // do stuff with message
}


// listen to test and test2 topics
client := kafkaclient.New("localhost", "group", onMessageReceived)
go client.ListenToTopics([]string{"test", "test2"})
```

## Producer
```
client := kafkaclient.New("localhost", "producerGroup", nil)
// produce message to test topic
go client.ProduceToTopic("test", "this is a test message")
```
