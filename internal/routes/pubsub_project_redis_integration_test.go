package routes

import (
 "net/http"
 "os"
 "strings"
 "testing"
)

// TestProjectRedisGraphIsolation exercises the real HTTP handlers and Redis
// namespace separation with distinct verified project bindings.
func TestProjectRedisGraphIsolation(t *testing.T) {
 addr:=os.Getenv("LOGMA_PUBSUBMODEL_REDIS_ADDR")
 if addr=="" {t.Skip("LOGMA_PUBSUBMODEL_REDIS_ADDR is not set")}
 t.Setenv("API_KEY","test-api-key")
 t.Setenv("PUBSUB_RESOURCE_REDIS_URI",addr)
 t.Setenv("FATLINE_SCOPE","agni-project-isolation")
 mk:=func(project string) http.Handler {
  policy:=fixedProjectBinding{project:project,allow:map[string]bool{}}
  for _,action:=range []string{"write","read","list","delete"} {
   for _,id:=range []string{"collection","shared"} {
    policy.allow[action+"|project/"+project+"/pubsub/channels/"+id]=true
   }
  }
  api:=&pubSubResourceAPI{projectResolver:fixedProjectResolver{binding:policy},projectStore:newPubSubResourceRedisStoreForProject}
  return newPubSubResourceRouter(api)
 }
 alpha:=mk("alpha")
 beta:=mk("beta")
 if resp:=requestResource(t,alpha,http.MethodPost,"/pubsub/channels",`{"name":"shared"}`);resp.Code!=201 {t.Fatalf("alpha create: %d %s",resp.Code,resp.Body.String())}
 if resp:=requestResource(t,beta,http.MethodGet,"/pubsub/channels/shared","");resp.Code!=404 {t.Fatalf("beta cross-project read: %d %s",resp.Code,resp.Body.String())}
 if resp:=requestResource(t,beta,http.MethodGet,"/pubsub/channels","");resp.Code!=200 || strings.Contains(resp.Body.String(),"shared") {t.Fatalf("beta enumeration: %d %s",resp.Code,resp.Body.String())}
 if resp:=requestResource(t,beta,http.MethodDelete,"/pubsub/channels/shared","");resp.Code!=204 {t.Fatalf("beta isolated delete: %d %s",resp.Code,resp.Body.String())}
 alpha=mk("alpha") // fresh router and Redis connection
 if resp:=requestResource(t,alpha,http.MethodGet,"/pubsub/channels/shared","");resp.Code!=200 {t.Fatalf("alpha reconnect: %d %s",resp.Code,resp.Body.String())}
 if resp:=requestResource(t,alpha,http.MethodDelete,"/pubsub/channels/shared","");resp.Code!=204 {t.Fatalf("alpha cleanup: %d %s",resp.Code,resp.Body.String())}
}
