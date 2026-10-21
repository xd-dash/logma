package routes

import (
 "net/http"
 "net/http/httptest"
 "context"
 "errors"
 "github.com/xd-dash/logma/serverless/authhttp"
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

type integrationBinding struct{ project string }
func (b integrationBinding) ProjectID() string {return b.project}
func (b integrationBinding) Authorize(action,resource string) error {
 for _,id:=range []string{"collection","shared"} {
  for _,a:=range []string{"write","read","list","delete"} {
   if a==action && resource=="project/"+b.project+"/pubsub/channels/"+id {return nil}
  }
 }
 return errors.New("denied")
}
// integrationVerifier is an isolated test double, not Prajapati credential verification.
type integrationVerifier struct{}
func (integrationVerifier) Resolve(_ context.Context, credential,audience string)(authhttp.Binding,error) {
 if audience!="logma" {return nil,errors.New("wrong audience")}
 switch credential {
 case "alpha-secret": return integrationBinding{project:"alpha"},nil
 case "beta-secret": return integrationBinding{project:"beta"},nil
 default: return nil,errors.New("invalid credential")
 }
}
func TestAuthenticatedProjectRedisIsolation(t *testing.T) {
 addr:=os.Getenv("LOGMA_PUBSUBMODEL_REDIS_ADDR")
 if addr=="" {t.Skip("LOGMA_PUBSUBMODEL_REDIS_ADDR is not set")}
 t.Setenv("PUBSUB_RESOURCE_REDIS_URI",addr)
 t.Setenv("FATLINE_SCOPE","agni-authenticated-project-isolation")
 mk:=func() http.Handler{return NewPubSubResourceRouterWithHTTPBinding("logma",integrationVerifier{})}
 h:=mk()
 request:=func(handler http.Handler,token,method,path,body string)*httptest.ResponseRecorder{
  t.Helper()
  req:=httptest.NewRequest(method,path,strings.NewReader(body))
  if token!="" {req.Header.Set("Authorization","Bearer "+token)}
  req.Header.Set("Content-Type","application/json")
  rr:=httptest.NewRecorder()
  handler.ServeHTTP(rr,req)
  return rr
 }
 if r:=request(h,"",http.MethodGet,"/pubsub/channels","");r.Code!=401 {t.Fatalf("missing token: %d",r.Code)}
 if r:=request(h,"invalid",http.MethodGet,"/pubsub/channels","");r.Code!=401 {t.Fatalf("invalid token: %d",r.Code)}
 if r:=request(h,"alpha-secret",http.MethodPost,"/pubsub/channels",`{"name":"shared"}`);r.Code!=201 {t.Fatalf("alpha create: %d %s",r.Code,r.Body.String())}
 if r:=request(h,"beta-secret",http.MethodGet,"/pubsub/channels/shared","");r.Code!=404 {t.Fatalf("beta read: %d %s",r.Code,r.Body.String())}
 if r:=request(h,"beta-secret",http.MethodGet,"/pubsub/channels","");r.Code!=200 || strings.Contains(r.Body.String(),"shared") {t.Fatalf("beta list: %d %s",r.Code,r.Body.String())}
 h=mk()
 if r:=request(h,"alpha-secret",http.MethodGet,"/pubsub/channels/shared","");r.Code!=200 {t.Fatalf("alpha reconnect: %d %s",r.Code,r.Body.String())}
 if r:=request(h,"alpha-secret",http.MethodDelete,"/pubsub/channels/shared","");r.Code!=204 {t.Fatalf("alpha cleanup: %d %s",r.Code,r.Body.String())}
}
