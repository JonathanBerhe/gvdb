//go:build e2e
// +build e2e

/*
Copyright 2026 GVDB.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// End-to-end test for the operator-driven backup/restore flow on a
// real kind cluster. This is the only test that exercises the full
// stack: operator reconciler → cluster CRDs → rendered StatefulSets →
// running GVDB binaries → backup target on a PVC.
//
// Preconditions (caller's responsibility, satisfied by `make test-e2e`
// at the operator/ level after `make docker-build` at repo root):
//   1. A kind cluster named gvdb-operator-test-e2e is running.
//   2. The gvdb-operator image is built and loaded into kind. The
//      enclosing BeforeSuite handles this.
//   3. The gvdb (server) image `gvdb:latest` is built and loaded into
//      kind. The test's own BeforeAll attempts this; it skips the test
//      with a clear message if the image is unavailable.

package e2e

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "gvdb/operator/internal/gvdbpb"
	"gvdb/operator/test/utils"
)

const (
	gvdbServerImage = "gvdb:latest"
	clusterNS       = "gvdb-e2e"
	clusterName     = "e2e"
	collectionName  = "e2e_products"
	dimension       = uint32(4)
	vectorCount     = 8
)

var _ = Describe("Backup and Restore via CRs", Ordered, func() {
	var (
		proxyForwardCmd *exec.Cmd
		proxyAddr       string
	)

	BeforeAll(func() {
		By("verifying the gvdb server image is loaded into kind")
		// The host build pipeline (`make docker-build` at repo root)
		// produces gvdb:latest. The operator test harness loads it via
		// kind. If the image is missing we surface a clear skip so
		// `make test-e2e` doesn't fail opaquely on a fresh checkout.
		err := utils.LoadImageToKindClusterWithName(gvdbServerImage)
		if err != nil {
			Skip(fmt.Sprintf(
				"gvdb server image not available (run `make docker-build` "+
					"at repo root first): %v", err))
		}

		By("waiting for any in-flight manager-namespace termination to finish")
		// The Manager test (kubebuilder boilerplate) runs first in this
		// suite and its AfterAll deletes `gvdb-operator-system` with
		// --wait=false. If we `make deploy` into a still-Terminating
		// namespace, K8s silently refuses to create the Deployment
		// (NamespaceTerminating admission denial) and the controller
		// pod never appears. Wait until the namespace is gone or back
		// to Active before proceeding.
		Eventually(func() string {
			out, _ := utils.Run(exec.Command("kubectl", "get", "ns",
				namespace, "--ignore-not-found",
				"-o", "jsonpath={.status.phase}"))
			return strings.TrimSpace(out)
		}, 90*time.Second, 2*time.Second).ShouldNot(Equal("Terminating"),
			"manager namespace stuck Terminating after prior test cleanup")

		By("installing CRDs")
		_, err = utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred())

		By("deploying the controller-manager")
		_, err = utils.Run(exec.Command(
			"make", "deploy", fmt.Sprintf("IMG=%s", managerImage)))
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the controller-manager pod to be Ready")
		Eventually(func() error {
			out, err := utils.Run(exec.Command("kubectl", "wait",
				"--for=condition=Ready", "pod",
				"-l", "control-plane=controller-manager",
				"-n", namespace, "--timeout=2m"))
			if err != nil {
				return fmt.Errorf("%s: %w", out, err)
			}
			return nil
		}, 3*time.Minute, 5*time.Second).Should(Succeed())

		By("creating the test namespace")
		_, _ = utils.Run(exec.Command("kubectl", "create", "ns", clusterNS))

		By("applying the GVDBCluster manifest")
		// utils.Run chdir's to the operator project root before
		// invoking the command, so the path is rooted there.
		_, err = utils.Run(exec.Command("kubectl", "apply",
			"-f", "test/e2e/manifests/gvdbcluster.yaml"))
		Expect(err).NotTo(HaveOccurred())

		By("waiting for cluster pods to be Ready")
		// The operator labels pods with `app=<cluster>-<component>`
		// (see render.SelectorLabels). Pods start sequentially; allow
		// 4 min on a cold image-load path.
		for _, role := range []string{
			"coordinator", "data-node", "query-node", "proxy",
		} {
			label := fmt.Sprintf("app=%s-%s", clusterName, role)
			Eventually(func() error {
				out, err := utils.Run(exec.Command("kubectl", "wait",
					"--for=condition=Ready", "pod",
					"-l", label,
					"-n", clusterNS, "--timeout=2m"))
				if err != nil {
					return fmt.Errorf("waiting on %s: %s: %w", role, out, err)
				}
				return nil
			}, 4*time.Minute, 5*time.Second).Should(Succeed(),
				"%s pods never became Ready", role)
		}

		By("port-forwarding the proxy service")
		// Pick a free local port to avoid collisions with whatever else
		// might be running on the developer's machine.
		port, err := pickFreeLocalPort()
		Expect(err).NotTo(HaveOccurred())
		proxyAddr = fmt.Sprintf("127.0.0.1:%d", port)
		svcName := fmt.Sprintf("%s-proxy", clusterName)
		proxyForwardCmd = exec.Command("kubectl", "port-forward",
			"-n", clusterNS, "svc/"+svcName,
			fmt.Sprintf("%d:50050", port))
		Expect(proxyForwardCmd.Start()).To(Succeed())

		Eventually(func() error {
			conn, dialErr := net.DialTimeout("tcp", proxyAddr, time.Second)
			if dialErr != nil {
				return dialErr
			}
			_ = conn.Close()
			return nil
		}, 30*time.Second, 500*time.Millisecond).Should(Succeed())
	})

	AfterAll(func() {
		if proxyForwardCmd != nil && proxyForwardCmd.Process != nil {
			_ = proxyForwardCmd.Process.Kill()
		}
		By("collecting diagnostics on failure")
		if CurrentSpecReport().Failed() {
			out, _ := utils.Run(exec.Command("kubectl", "get",
				"all,gvdbcluster,gvdbbackup,gvdbrestore",
				"-n", clusterNS, "-o", "wide"))
			fmt.Fprintf(GinkgoWriter, "Cluster state:\n%s\n", out)
			// CR status conditions hold the actual failure reason
			// (e.g. StartBackupFailed, server ErrorMessage). The
			// "-o wide" view above shows phase only.
			for _, kind := range []string{"gvdbbackup", "gvdbrestore"} {
				out, _ = utils.Run(exec.Command("kubectl", "get", kind,
					"-n", clusterNS, "-o", "yaml"))
				fmt.Fprintf(GinkgoWriter, "%s yaml:\n%s\n", kind, out)
			}
			// Operator logs: backup/restore reconciler emits the
			// underlying error before it transitions the CR to Failed.
			out, _ = utils.Run(exec.Command("kubectl", "logs",
				"deployment/gvdb-operator-controller-manager",
				"-n", namespace, "--tail=200"))
			fmt.Fprintf(GinkgoWriter, "Operator logs:\n%s\n", out)
			// Coordinator drives the backup; its logs name the failing
			// shard / target. Data-node logs show write-path errors.
			for _, role := range []string{"coordinator", "data-node", "proxy"} {
				out, _ = utils.Run(exec.Command("kubectl", "logs",
					"-l", fmt.Sprintf("app=%s-%s", clusterName, role),
					"-n", clusterNS, "--tail=100"))
				fmt.Fprintf(GinkgoWriter, "%s logs:\n%s\n", role, out)
			}
		}
		By("deleting the GVDBCluster + test namespace")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", clusterNS,
			"--wait=false", "--timeout=30s"))
		By("undeploying the controller-manager (bounded)")
		// Without explicit timeouts, `kubectl delete` here will block
		// for the default --grace-period=30s × every-resource window
		// and can hang for minutes if any pod's finalizer is slow.
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns",
			"gvdb-operator-system", "--wait=false", "--timeout=30s"))
	})

	It("backs up a collection via GVDBBackup and restores it via GVDBRestore", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()

		By("connecting to the cluster via the port-forwarded proxy")
		conn, err := grpc.NewClient(proxyAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		Expect(err).NotTo(HaveOccurred())
		defer conn.Close()
		client := pb.NewVectorDBServiceClient(conn)

		By("creating a collection (retry to absorb port-forward race)")
		// kubectl port-forward accepts the local TCP connection
		// before the upstream bridge is ready, so the first gRPC
		// call after Dial often hits a connection-refused or
		// upstream-EOF. Retry for up to 30 s.
		Eventually(func() error {
			rpcCtx, rpcCancel := context.WithTimeout(ctx, 5*time.Second)
			defer rpcCancel()
			_, callErr := client.CreateCollection(rpcCtx,
				&pb.CreateCollectionRequest{
					CollectionName: collectionName,
					Dimension:      dimension,
					Metric:         pb.CreateCollectionRequest_L2,
					IndexType:      pb.CreateCollectionRequest_FLAT,
				})
			if callErr != nil {
				fmt.Fprintf(GinkgoWriter,
					"CreateCollection retry: %v\n", callErr)
			}
			return callErr
		}, 30*time.Second, 2*time.Second).Should(Succeed(),
			"CreateCollection never succeeded through port-forward")

		By("inserting vectors (retry to absorb heartbeat propagation)")
		// CreateCollection returns once the coordinator allocates shards,
		// but the data-node only learns it is primary for shard 0 on the
		// next heartbeat — heartbeat_sender sleeps 10 s between ticks. An
		// Insert fired in the gap hits an empty PrimaryTermTracker and is
		// rejected with FAILED_PRECONDITION ("no primary record for shard
		// 0; re-route via RouteQuery"), which the proxy does not retry.
		// Retry from the client until the heartbeat populates the tracker.
		vectors := make([]*pb.VectorWithId, vectorCount)
		for i := uint64(0); i < uint64(vectorCount); i++ {
			values := make([]float32, dimension)
			for d := uint32(0); d < dimension; d++ {
				values[d] = float32(i*10) + float32(d)
			}
			vectors[i] = &pb.VectorWithId{
				Id:     i + 1,
				Vector: &pb.Vector{Values: values, Dimension: dimension},
			}
		}
		Eventually(func() error {
			rpcCtx, rpcCancel := context.WithTimeout(ctx, 5*time.Second)
			defer rpcCancel()
			_, callErr := client.Insert(rpcCtx, &pb.InsertRequest{
				CollectionName: collectionName,
				Vectors:        vectors,
			})
			if callErr != nil {
				fmt.Fprintf(GinkgoWriter,
					"Insert retry: %v\n", callErr)
			}
			return callErr
		}, 30*time.Second, 2*time.Second).Should(Succeed(),
			"Insert never succeeded after primary-term sync")

		By("applying a GVDBBackup CR with a local target on the data PVC")
		backupYAML := fmt.Sprintf(`apiVersion: gvdb.io/v1alpha1
kind: GVDBBackup
metadata:
  name: e2e-backup
  namespace: %s
spec:
  clusterRef:
    name: %s
  collection: %s
  target:
    local:
      path: /data/gvdb/backups
`, clusterNS, clusterName, collectionName)
		applyManifest(backupYAML)

		By("waiting for the GVDBBackup CR to reach Completed")
		Eventually(func() (string, error) {
			out, err := utils.Run(exec.Command("kubectl", "get", "gvdbbackup",
				"e2e-backup", "-n", clusterNS,
				"-o", "jsonpath={.status.phase}"))
			return strings.TrimSpace(out), err
		}, 2*time.Minute, 5*time.Second).Should(Equal("Completed"))

		By("dropping the source collection")
		_, err = client.DropCollection(ctx, &pb.DropCollectionRequest{
			CollectionName: collectionName,
		})
		Expect(err).NotTo(HaveOccurred())

		By("applying a GVDBRestore CR referencing the backup")
		restoreYAML := fmt.Sprintf(`apiVersion: gvdb.io/v1alpha1
kind: GVDBRestore
metadata:
  name: e2e-restore
  namespace: %s
spec:
  clusterRef:
    name: %s
  fromBackupRef:
    name: e2e-backup
  targetCollection: %s_restored
`, clusterNS, clusterName, collectionName)
		applyManifest(restoreYAML)

		By("waiting for the GVDBRestore CR to reach Completed")
		Eventually(func() (string, error) {
			out, err := utils.Run(exec.Command("kubectl", "get", "gvdbrestore",
				"e2e-restore", "-n", clusterNS,
				"-o", "jsonpath={.status.phase}"))
			return strings.TrimSpace(out), err
		}, 2*time.Minute, 5*time.Second).Should(Equal("Completed"))

		By("verifying the restored collection is searchable (retry for heartbeat sync)")
		// GVDBRestore reaches Completed when the server reports the
		// restore done, but the data-node only learns it owns the new
		// (restored) collection's shards on the next heartbeat tick —
		// same race as Insert after CreateCollection. Retry until the
		// tracker is populated.
		queryValues := make([]float32, dimension)
		queryID := uint64(3)
		for d := uint32(0); d < dimension; d++ {
			queryValues[d] = float32((queryID-1)*10) + float32(d)
		}
		var searchResp *pb.SearchResponse
		Eventually(func() error {
			rpcCtx, rpcCancel := context.WithTimeout(ctx, 5*time.Second)
			defer rpcCancel()
			resp, callErr := client.Search(rpcCtx, &pb.SearchRequest{
				CollectionName: collectionName + "_restored",
				QueryVector:    &pb.Vector{Values: queryValues, Dimension: dimension},
				TopK:           1,
			})
			if callErr != nil {
				fmt.Fprintf(GinkgoWriter,
					"Search retry: %v\n", callErr)
				return callErr
			}
			searchResp = resp
			return nil
		}, 30*time.Second, 2*time.Second).Should(Succeed(),
			"Search never succeeded on restored collection")
		Expect(searchResp.Results).NotTo(BeEmpty())
		Expect(searchResp.Results[0].Id).To(Equal(queryID),
			"top result should be the inserted vector")
	})
})

// applyManifest pipes a YAML literal into `kubectl apply -f -`. Used
// instead of materializing a temp file so the test stays self-contained.
func applyManifest(yaml string) {
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(yaml)
	out, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(),
		"kubectl apply failed: %s", out)
}

// pickFreeLocalPort opens a transient listener on :0 to learn a port
// the OS considers free, then closes it. There's an inherent race
// between close and re-bind by kubectl port-forward, but in practice
// the macOS/Linux ephemeral-port allocator doesn't re-issue the same
// port within a second.
func pickFreeLocalPort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
