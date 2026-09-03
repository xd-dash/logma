package pubsubruntime

import (
	"context"
	"reflect"
	"testing"

	"github.com/xd-dash/logma/internal/pubsubmodel"
)

type publisherTestStore struct {
	publisher pubsubmodel.Publisher
	channel   pubsubmodel.Channel
}
func (s publisherTestStore) GetPublisher(context.Context,string)(pubsubmodel.Publisher,error){return s.publisher,nil}
func (s publisherTestStore) GetChannel(context.Context,string)(pubsubmodel.Channel,error){return s.channel,nil}

type publisherTestActivator struct { active bool; events *[]string }
func (a *publisherTestActivator) Active(string) bool { return a.active }
func (a *publisherTestActivator) Activate(context.Context,string,func(string))(*Handle,error){*a.events=append(*a.events,"channel");a.active=true;return nil,nil}

type publisherTestProvider struct{ events *[]string }
func (p publisherTestProvider) EnsureActive(context.Context,pubsubmodel.Publisher,pubsubmodel.Channel)error{*p.events=append(*p.events,"publisher");return nil}

func TestPublisherReconcilerActivatesChannelBeforeProvider(t *testing.T){
	events:=[]string{}
	store:=publisherTestStore{publisher:pubsubmodel.Publisher{ID:"stonks-live",Channel:"market:quotes",Type:"stonks"},channel:pubsubmodel.Channel{Name:"market:quotes"}}
	activator:=&publisherTestActivator{events:&events}
	registry:=NewPublisherRegistry()
	if err:=registry.Register("stonks",publisherTestProvider{events:&events});err!=nil{t.Fatal(err)}
	reconciler,err:=NewPublisherReconciler(store,activator,registry);if err!=nil{t.Fatal(err)}
	if err:=reconciler.Reconcile(context.Background(),"stonks-live");err!=nil{t.Fatal(err)}
	if want:=[]string{"channel","publisher"};!reflect.DeepEqual(events,want){t.Fatalf("events=%v want %v",events,want)}
}

func TestPublisherReconcilerReusesActiveChannel(t *testing.T){
	events:=[]string{}
	store:=publisherTestStore{publisher:pubsubmodel.Publisher{ID:"news",Channel:"news",Type:"news"},channel:pubsubmodel.Channel{Name:"news"}}
	activator:=&publisherTestActivator{active:true,events:&events}
	registry:=NewPublisherRegistry();if err:=registry.Register("news",publisherTestProvider{events:&events});err!=nil{t.Fatal(err)}
	reconciler,err:=NewPublisherReconciler(store,activator,registry);if err!=nil{t.Fatal(err)}
	if err:=reconciler.Reconcile(context.Background(),"news");err!=nil{t.Fatal(err)}
	if want:=[]string{"publisher"};!reflect.DeepEqual(events,want){t.Fatalf("events=%v want %v",events,want)}
}

func TestPublisherRegistryRejectsDuplicateProvider(t *testing.T){
	registry:=NewPublisherRegistry();provider:=publisherTestProvider{events:&[]string{}}
	if err:=registry.Register("stonks",provider);err!=nil{t.Fatal(err)}
	if err:=registry.Register("stonks",provider);err==nil{t.Fatal("duplicate provider registration succeeded")}
}
