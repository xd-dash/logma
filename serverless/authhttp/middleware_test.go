package authhttp

import (
 "context"
 "errors"
 "net/http"
 "net/http/httptest"
 "testing"
)

type testBinding struct{ project string }
func (b testBinding) ProjectID() string { return b.project }
func (b testBinding) Authorize(action, resource string) error {
 if action=="read" && resource=="project/alpha/pubsub/channels/events" {return nil}
 return errors.New("forbidden")
}
type testResolver struct{ binding Binding; err error; called *int }
func (v testResolver) Resolve(_ context.Context, token, audience string)(Binding,error) {
(*v.called)++
 if token!="valid-token" || audience!="logma" {return nil, errors.New("invalid")}
 return v.binding,v.err
}
func TestMiddleware(t *testing.T) {
 calls:=0
 handler:=Middleware("logma",testResolver{binding:testBinding{project:"alpha"},called:&calls})(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
  b,ok:=FromContext(r.Context())
  if !ok || b.ProjectID()!="alpha" {t.Error("verified binding not propagated")}
  if b.Authorize("read","project/beta/pubsub/channels/events")==nil {t.Error("cross-project access permitted")}
  w.WriteHeader(http.StatusNoContent)
 }))
 for _,tc:=range []struct{name,header string;status int}{
  {"missing","",401},{"wrong-scheme","Basic abc",401},{"invalid-token","Bearer invalid",401},{"valid","Bearer valid-token",204},
 } {
  t.Run(tc.name,func(t *testing.T){
   req:=httptest.NewRequest("GET","/",nil)
   if tc.header!="" {req.Header.Set("Authorization",tc.header)}
   resp:=httptest.NewRecorder()
   handler.ServeHTTP(resp,req)
   if resp.Code!=tc.status {t.Fatalf("status %d, expected %d",resp.Code,tc.status)}
  })
 }
 if calls!=2 {t.Fatalf("resolver called %d times, want 2",calls)}
}
func TestMiddlewareRejectsAmbiguousHeadersAndUnavailableAuthority(t *testing.T) {
 n:=0
 resolver:=testResolver{binding:testBinding{project:"alpha"},called:&n}
 for _,handler:=range []http.Handler{
  Middleware("",resolver)(http.NotFoundHandler()),
  Middleware("logma",nil)(http.NotFoundHandler()),
 } {
  rr:=httptest.NewRecorder()
  req:=httptest.NewRequest("GET","/",nil)
  req.Header.Set("Authorization","Bearer valid-token")
  handler.ServeHTTP(rr,req)
  if rr.Code!=503 {t.Fatalf("unavailable authority: %d",rr.Code)}
 }
 handler:=Middleware("logma",resolver)(http.NotFoundHandler())
 req:=httptest.NewRequest("GET","/",nil)
 req.Header.Add("Authorization","Bearer valid-token")
 req.Header.Add("Authorization","Bearer valid-token")
 rr:=httptest.NewRecorder()
 handler.ServeHTTP(rr,req)
 if rr.Code!=401 || n!=0 {t.Fatalf("duplicate headers accepted: %d calls=%d",rr.Code,n)}
}

func TestAuthenticateCallbackForSkymill(t *testing.T) {
 calls:=0
 authenticate:=Authenticate("logma",testResolver{binding:testBinding{project:"alpha"},called:&calls},func(r *http.Request,b Binding)(context.Context,error){
  from,ok:=FromContext(r.Context())
  if !ok || from.ProjectID()!=b.ProjectID() {return nil,errors.New("binding lost")}
  return r.Context(),nil
 })
 req:=httptest.NewRequest("POST","/v1/messages",nil)
 req.Header.Set("Authorization","Bearer valid-token")
 ctx,err:=authenticate(req)
 if err!=nil {t.Fatal(err)}
 b,ok:=FromContext(ctx)
 if !ok || b.ProjectID()!="alpha" {t.Fatal("verified context unavailable")}
 req.Header.Set("Authorization","Bearer wrong")
 if _,err:=authenticate(req);err==nil {t.Fatal("invalid credential accepted")}
 if calls!=2 {t.Fatalf("resolver calls: %d",calls)}
}
