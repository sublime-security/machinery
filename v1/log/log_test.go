package log_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/log"
)

func TestDefaultLogger(t *testing.T) {
	log.DEBUG.Print("should not panic")
	log.INFO.Print("should not panic")
	log.WARNING.Print("should not panic")
	log.ERROR.Print("should not panic")
	log.FATAL.Print("should not panic")
}
