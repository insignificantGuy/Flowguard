// Package handler exposes HTTP handlers for the API.
package handler

import (
	"encoding/json"
	"net/http"

	"flowguard/internal/response"
)

// OpenAPISpec is the OpenAPI 3.1 spec for the flowguard API.
//
// Kept inline (rather than loaded from a YAML file at runtime) so the docs
// endpoint has no file-system dependency in production containers.
const OpenAPISpec = `{
  "openapi": "3.1.0",
  "info": {
    "title": "flowguard API",
    "version": "1.0.0",
    "description": "Scalable web server API."
  },
  "servers": [{ "url": "/", "description": "current" }],
  "paths": {
    "/health": {
      "get": {
        "summary": "Liveness probe",
        "responses": {
          "200": {
            "description": "Server is healthy",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "data": {
                      "type": "object",
                      "properties": {
                        "status": { "type": "string" },
                        "uptimeSeconds": { "type": "number" },
                        "nodeEnv": { "type": "string" }
                      }
                    },
                    "traceId": { "type": "string" },
                    "timestamp": { "type": "string" }
                  }
                }
              }
            }
          }
        }
      }
    },
    "/v1/send-message": {
      "post": {
        "summary": "Send a message",
        "description": "Echoes a greeting combined with the provided message.",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["message"],
                "properties": {
                  "message": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 4096,
                    "description": "The message body to echo."
                  }
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Message accepted",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "data": {
                      "type": "object",
                      "properties": {
                        "echo": { "type": "string" },
                        "receivedAt": { "type": "string", "format": "date-time" }
                      }
                    },
                    "traceId": { "type": "string" },
                    "timestamp": { "type": "string" }
                  }
                }
              }
            }
          },
          "400": { "$ref": "#/components/responses/ValidationError" },
          "429": { "$ref": "#/components/responses/RateLimitError" }
        }
      }
    }
  },
  "components": {
    "responses": {
      "ValidationError": {
        "description": "Request failed validation",
        "content": {
          "application/json": {
            "schema": {
              "type": "object",
              "properties": {
                "error": {
                  "type": "object",
                  "properties": {
                    "code": { "type": "string", "example": "VALIDATION_ERROR" },
                    "message": { "type": "string" },
                    "traceId": { "type": "string" },
                    "details": {}
                  }
                }
              }
            }
          }
        }
      },
      "RateLimitError": {
        "description": "Too many requests",
        "content": {
          "application/json": {
            "schema": {
              "type": "object",
              "properties": {
                "error": {
                  "type": "object",
                  "properties": {
                    "code": { "type": "string", "example": "RATE_LIMITED" },
                    "message": { "type": "string" },
                    "traceId": { "type": "string" }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
`

// OpenAPI serves the OpenAPI spec at /openapi.json.
type OpenAPI struct{}

// NewOpenAPI creates an OpenAPI handler.
func NewOpenAPI() *OpenAPI { return &OpenAPI{} }

// ServeHTTP implements http.Handler.
func (h *OpenAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		response.Error(w, r, http.StatusMethodNotAllowed,
			"METHOD_NOT_ALLOWED", "Use GET", nil)
		return
	}
	var spec any
	_ = json.Unmarshal([]byte(OpenAPISpec), &spec)
	response.Send(w, r, http.StatusOK, spec)
}