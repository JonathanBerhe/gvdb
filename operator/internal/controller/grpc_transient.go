/*
Copyright 2026 GVDB.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// isTransientGRPCError reports whether err is a gRPC failure worth
// retrying without surfacing it as a terminal CR phase. UNAVAILABLE
// (connection refused, no endpoint, RST) and DEADLINE_EXCEEDED (slow
// path) both clear up on their own once the proxy or coordinator is
// fully online, and dominate the error surface during the few seconds
// between pods reaching Ready and kube-proxy programming service
// endpoints. Every other code is treated as terminal: caller-side
// argument errors, permission failures, NotFound, internal bugs all
// re-produce the same error on retry.
func isTransientGRPCError(err error) bool {
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch s.Code() {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	}
	return false
}
