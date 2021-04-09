package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"

	"EXP/pkg/actor"
)

type PipeData struct {
	Annotations []string
	Data        interface{}
}

func actorURLFetcher(ctx context.Context, rx actor.MsgRX) actor.Msg {
	if rx.Done {
		return actor.Msg{}
	}

	url := rx.Msg.Data.(string)
	rx.SetBaggage("url", url)

	r, err := http.Get(rx.Msg.Data.(string))
	if err != nil {
		return actor.Msg{Error: err}
	}

	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return actor.Msg{Error: err}
	}

	return actor.Msg{
		Data: bodyBytes,
	}
}

func actorBodyTrimmer(ctx context.Context, rx actor.MsgRX) actor.Msg {
	if rx.Done {
		return actor.Msg{}
	}
	if rx.Msg.Error != nil {
		return rx.Msg
	}
	return actor.Msg{
		Data: rx.Msg.Data.([]byte)[0:100],
	}
}

func actorSink(data map[string]string) actor.MsgProcessor {
	return func(ctx context.Context, rx actor.MsgRX) actor.Msg {
		rx.SetSink(true)
		if rx.Done {
			return actor.Msg{}
		}
		url := rx.GetBaggage("url").(string)

		if rx.Msg.Error != nil {
			data[url] = fmt.Sprint("SINK:ERR ", rx.Msg.Error)
			return actor.Msg{}
		}

		data[url] = string(rx.Msg.Data.([]byte))
		return actor.Msg{}
	}
}

func main() {
	input := make(chan actor.Msg)
	data := make(map[string]string)

	ctx := context.Background()
	named := func(name string) context.Context {
		return context.WithValue(ctx, "actor", name)
	}
	fetcherPH := actor.SpawnActorPool(
		named("fetcher"), 10,
		actorURLFetcher, input,
	).Observe(
		named("fetch-obs"), 1,
		func(ctx context.Context, rx actor.MsgRX) {
			fmt.Println("FETCHED", rx.GetBaggage("url"), rx.Msg.Error, rx.Done)
		},
	)
	trimmerPH := actor.SpawnActorPool(
		named("trimmer"), 10,
		actorBodyTrimmer, fetcherPH.Output,
	)
	sinkPH := actor.SpawnActorPool(
		named("sink"), 1,
		actorSink(data), trimmerPH.Output,
	)

	go func() {
		input <- actor.Msg{Data: "http://google.com"}
		input <- actor.Msg{Data: "https://uber.com"}
		input <- actor.Msg{Data: "http://soundcloud.com"}
		close(input)
	}()

	for msg := range sinkPH.Output {
		fmt.Println("ERR:", msg.Error)
	}

	fmt.Println("DONE")
	<-sinkPH.Wait

	fmt.Printf("DATA: %#v\n", data)
}
