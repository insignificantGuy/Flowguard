package observability

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerUsesItsOwnRegistry(t *testing.T) {
	a, b := NewTestMetrics(), NewTestMetrics()
	a.RecordPublished("a")
	b.RecordPublished("b")
	w := httptest.NewRecorder()
	a.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(w.Body.String(), `topic="a"`) || strings.Contains(w.Body.String(), `topic="b"`) {
		t.Fatal(w.Body.String())
	}
}
