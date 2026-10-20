package pubsubmodel

import (
 "context"
 "os"
 "strings"
 "testing"

 "github.com/redis/go-redis/v9"
)

// TestRedisStoreReconnectPersistence proves a fresh client/store can recover
// graph state without relying on client-local caches or connection identity.
func TestRedisStoreReconnectPersistence(t *testing.T) {
 addr:=os.Getenv("LOGMA_PUBSUBMODEL_REDIS_ADDR")
 if addr=="" {t.Skip("LOGMA_PUBSUBMODEL_REDIS_ADDR is not set")}
 ctx:=context.Background()
 first:=redis.NewClient(&redis.Options{Addr:addr})
 scope:="agni-reconnect-"+strings.ReplaceAll(t.Name(), "/", "-")
 store,err:=NewRedisStore(first,scope);if err!=nil {t.Fatal(err)}
 if err=store.PutChannel(ctx,Channel{Name:"reconnect-events"});err!=nil {t.Fatal(err)}
 if err=first.Close();err!=nil {t.Fatal(err)}
 second:=redis.NewClient(&redis.Options{Addr:addr})
 defer second.Close()
 restored,err:=NewRedisStore(second,scope);if err!=nil {t.Fatal(err)}
 value,err:=restored.GetChannel(ctx,"reconnect-events")
 if err!=nil || value.Name!="reconnect-events" {t.Fatalf("reconnect returned %#v, %v",value,err)}
 if err=restored.DeleteChannel(ctx,"reconnect-events");err!=nil {t.Fatal(err)}
 ids,err:=restored.ChannelIDs(ctx);if err!=nil || len(ids)!=0 {t.Fatalf("residual channel registry: %v %v",ids,err)}
}
