package actions

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/go-chi/chi"
)

func makeRequest(
	t *testing.T,
	queryParams map[string]string,
	routeParams map[string]string,
) *http.Request {
	request, err := http.NewRequest("GET", "/", nil)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	query := url.Values{}
	for key, value := range queryParams {
		query.Set(key, value)
	}
	request.URL.RawQuery = query.Encode()

	chiRouteContext := chi.NewRouteContext()
	for key, value := range routeParams {
		chiRouteContext.URLParams.Add(key, value)
	}
	ctx := context.WithValue(context.Background(), chi.RouteCtxKey, chiRouteContext)
	return request.WithContext(ctx)
}
