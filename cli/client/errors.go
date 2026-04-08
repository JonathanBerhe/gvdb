package client

import (
	"fmt"

	"google.golang.org/grpc/status"
)

func FormatError(err error) string {
	st, ok := status.FromError(err)
	if ok {
		return fmt.Sprintf("[%s] %s", st.Code(), st.Message())
	}
	return err.Error()
}
