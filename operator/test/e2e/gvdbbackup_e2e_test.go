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
		_, err = utils.Run(exec.Command("kubectl", "apply",
			"-f", "manifests/gvdbcluster.yaml"))
		Expect(err).NotTo(HaveOccurred())

		By("waiting for cluster pods to be Ready")
		// The operator reconciles the GVDBCluster into a StatefulSet
		// per role + a Deployment for the proxy + a ConfigMap. Pods
		// start sequentially; allow 4 min on a cold image-load path.
		for _, role := range []string{
			"coordinator", "data-node", "query-node", "proxy",
		} {
			label := fmt.Sprintf("app.kubernetes.io/component=%s", role)
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
		svcName := fmt.Sprintf("gvdbcluster-%s-proxy", clusterName)
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
		}
		By("deleting the GVDBCluster + test namespace")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", clusterNS,
			"--wait=false"))
		By("undeploying the controller-manager")
		_, _ = utils.Run(exec.Command("make", "undeploy"))
		_, _ = utils.Run(exec.Command("make", "uninstall"))
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

		By("creating a collection and inserting vectors")
		_, err = client.CreateCollection(ctx, &pb.CreateCollectionRequest{
			CollectionName: collectionName,
			Dimension:      dimension,
			Metric:         pb.CreateCollectionRequest_L2,
			IndexType:      pb.CreateCollectionRequest_FLAT,
		})
		Expect(err).NotTo(HaveOccurred())

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
		_, err = client.Insert(ctx, &pb.InsertRequest{
			CollectionName: collectionName,
			Vectors:        vectors,
		})
		Expect(err).NotTo(HaveOccurred())

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

		By("verifying the restored collection is searchable")
		queryValues := make([]float32, dimension)
		queryID := uint64(3)
		for d := uint32(0); d < dimension; d++ {
			queryValues[d] = float32((queryID-1)*10) + float32(d)
		}
		searchResp, err := client.Search(ctx, &pb.SearchRequest{
			CollectionName: collectionName + "_restored",
			QueryVector:    &pb.Vector{Values: queryValues, Dimension: dimension},
			TopK:           1,
		})
		Expect(err).NotTo(HaveOccurred())
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
