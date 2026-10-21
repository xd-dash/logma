package routes

import (
 "context"
 "errors"
 "net/http"
 "net/http/httptest"
 "strings"
 "testing"
)

type fixedProjectResolver struct {
 binding VerifiedProjectBinding
 err error
}
func (r fixedProjectResolver) ResolveProjectBinding(context.Context) (VerifiedProjectBinding, error) { return r.binding,r.err }

type fixedProjectBinding struct {
 project string
 allow map[string]bool
}
func (b fixedProjectBinding) ProjectID() string { return b.project }
func (b fixedProjectBinding) Authorize(action, resource string) error {
 if b.allow[action+"|"+resource] { return nil }
 return errors.New("denied")
}

func TestPubSubVerifiedProjectBoundary(t *testing.T) {
 t.Setenv("API_KEY", "test-api-key")
 store:=newFakePubSubResourceStore()
 // This fixture represents authority after a trusted Prajapati binding evaluator
 // has verified the principal, policy digest and project.
 a:=&pubSubResourceAPI{store:func()(pubSubResourceStore,error){return store,nil}, projectStore:func(string)(pubSubResourceStore,error){return store,nil}}
 project:="alpha"
 resource:="project/alpha/pubsub/channels/collection"
 a.projectResolver=fixedProjectResolver{binding:fixedProjectBinding{
  project:project, allow:map[string]bool{"write|"+resource:true,
    "list|"+resource:true,"read|project/alpha/pubsub/channels/events":true},
 }}
 router:=newPubSubResourceRouter(a)
 request:=func(method,path,body string) *httptest.ResponseRecorder {
  req:=httptest.NewRequest(method,path,strings.NewReader(body))
  req.Header.Set(apiKeyHeader,"test-api-key")
  w:=httptest.NewRecorder()
  router.ServeHTTP(w,req)
  return w
 }
 if got:=request(http.MethodPost,"/pubsub/channels",`{"name":"events"}`);got.Code!=201 { t.Fatalf("allowed write=%d: %s",got.Code,got.Body.String()) }
 if got:=request(http.MethodGet,"/pubsub/channels/events","");got.Code!=200 {t.Fatalf("allowed read=%d",got.Code)}
 if got:=request(http.MethodDelete,"/pubsub/channels/events","");got.Code!=403 {t.Fatalf("denied delete=%d",got.Code)}
 if got:=request(http.MethodGet,"/pubsub/callbacks","");got.Code!=403 {t.Fatalf("cross-resource list=%d",got.Code)}
 a.projectResolver=fixedProjectResolver{binding:fixedProjectBinding{project:"beta",allow:map[string]bool{}}}
 router=newPubSubResourceRouter(a)
 if got:=request(http.MethodGet,"/pubsub/channels/events","");got.Code!=403 {t.Fatalf("cross-project read=%d",got.Code)}
 a.projectResolver=fixedProjectResolver{err:errors.New("no identity")}
 router=newPubSubResourceRouter(a)
 if got:=request(http.MethodGet,"/pubsub/channels/events","");got.Code!=403 {t.Fatalf("missing binding=%d",got.Code)}
}
