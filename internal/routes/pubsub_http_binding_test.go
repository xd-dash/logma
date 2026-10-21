package routes

import (
 "context"
 "errors"
 "net/http"
 "net/http/httptest"
 "testing"

 "github.com/xd-dash/logma/serverless/authhttp"
)

type httpTestResolver struct {}
type httpTestBinding struct{ project string }
func (b httpTestBinding) ProjectID() string {return b.project}
func (b httpTestBinding) Authorize(action,resource string) error {
 if b.project=="alpha" && action=="list" && resource=="project/alpha/pubsub/channels/collection" {return nil}
 return errors.New("denied")
}
func (httpTestResolver) Resolve(_ context.Context,token,audience string)(authhttp.Binding,error){
 if token=="alpha" && audience=="logma" {return httpTestBinding{project:"alpha"},nil}
 return nil,errors.New("unverified")
}
func TestProtectedHTTPRouterRejectsUnverifiedRequests(t *testing.T) {
 // The store is deliberately unconfigured: the accepted request should reach
 // the storage boundary, while denied requests must be rejected before it.
 h:=NewPubSubResourceRouterWithHTTPBinding("logma",httpTestResolver{})
 for _,tc:=range []struct{token string; want int}{
  {"",http.StatusUnauthorized},
  {"beta",http.StatusUnauthorized},
  {"alpha",http.StatusServiceUnavailable},
 } {
  r:=httptest.NewRequest(http.MethodGet,"/pubsub/channels",nil)
  if tc.token!="" {r.Header.Set("Authorization","Bearer "+tc.token)}
  w:=httptest.NewRecorder()
  h.ServeHTTP(w,r)
  if w.Code!=tc.want {t.Fatalf("%q got %d, want %d: %s",tc.token,w.Code,tc.want,w.Body.String())}
 }
}
