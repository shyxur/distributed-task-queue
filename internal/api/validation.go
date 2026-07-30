package api

import (
	"errors"
	"fmt"

	"github.com/go-playground/validator/v10"
)

var validate = validator.New()

func validateRequest(req *createTaskRequest) map[string]string {
	// Payload kontrolü
	if len(req.Payload) == 0 || string(req.Payload) == "null" {
		return map[string]string{"payload": "payload is required"}
	}

	err := validate.Struct(req)
	if err == nil {
		return nil
	}

	var ve validator.ValidationErrors
	if errors.As(err, &ve) {
		out := make(map[string]string)
		for _, fe := range ve {
			out[fe.Field()] = fmt.Sprintf("failed on tag: %s", fe.Tag())
		}
		return out
	}

	return map[string]string{"error": err.Error()}
}