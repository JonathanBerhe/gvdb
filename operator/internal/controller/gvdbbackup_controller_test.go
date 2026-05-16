/*
Copyright 2026 GVDB.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	gvdbv1alpha1 "gvdb/operator/api/v1alpha1"
	"gvdb/operator/internal/gvdbclient"
)

// GVDBBackup envtest. The proxy doesn't exist in the envtest harness, so
// the reconciler's StartBackup call will fail at the dial step — the
// reconciler is expected to mark the CR Failed with a clear reason. The
// tests below validate the CR-level reactions, not the gRPC happy path
// (that's covered by the C++ unit + integration suites).

var _ = Describe("GVDBBackup Controller", func() {
	Context("When reconciling a GVDBBackup", func() {
		const backupName = "test-backup"
		const clusterName = "test-cluster"
		const namespace = "default"

		ctx := context.Background()
		backupKey := types.NamespacedName{Name: backupName, Namespace: namespace}
		clusterKey := types.NamespacedName{Name: clusterName, Namespace: namespace}

		AfterEach(func() {
			b := &gvdbv1alpha1.GVDBBackup{}
			if err := k8sClient.Get(ctx, backupKey, b); err == nil {
				Expect(k8sClient.Delete(ctx, b)).To(Succeed())
			}
			c := &gvdbv1alpha1.GVDBCluster{}
			if err := k8sClient.Get(ctx, clusterKey, c); err == nil {
				Expect(k8sClient.Delete(ctx, c)).To(Succeed())
			}
		})

		It("rejects a target with both s3 and local set", func() {
			Expect(k8sClient.Create(ctx, &gvdbv1alpha1.GVDBCluster{
				ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
			})).To(Succeed())
			Expect(k8sClient.Create(ctx, &gvdbv1alpha1.GVDBBackup{
				ObjectMeta: metav1.ObjectMeta{Name: backupName, Namespace: namespace},
				Spec: gvdbv1alpha1.GVDBBackupSpec{
					ClusterRef: gvdbv1alpha1.LocalObjectReference{Name: clusterName},
					Collection: "products",
					Target: gvdbv1alpha1.BackupTargetSpec{
						S3:    &gvdbv1alpha1.S3BackupTarget{Bucket: "b"},
						Local: &gvdbv1alpha1.LocalBackupTarget{Path: "/p"},
					},
				},
			})).To(Succeed())

			r := &GVDBBackupReconciler{
				Client:    k8sClient,
				Scheme:    k8sClient.Scheme(),
				StatsPool: gvdbclient.NewPool(),
			}
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: backupKey})
			Expect(err).NotTo(HaveOccurred())

			got := &gvdbv1alpha1.GVDBBackup{}
			Expect(k8sClient.Get(ctx, backupKey, got)).To(Succeed())
			Expect(got.Status.Phase).To(Equal(gvdbv1alpha1.BackupPhaseFailed))
			Expect(got.Status.Conditions).NotTo(BeEmpty())
			Expect(got.Status.Conditions[0].Reason).To(Equal("InvalidTarget"))
		})

		It("rejects a target with neither s3 nor local set", func() {
			Expect(k8sClient.Create(ctx, &gvdbv1alpha1.GVDBCluster{
				ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
			})).To(Succeed())
			Expect(k8sClient.Create(ctx, &gvdbv1alpha1.GVDBBackup{
				ObjectMeta: metav1.ObjectMeta{Name: backupName, Namespace: namespace},
				Spec: gvdbv1alpha1.GVDBBackupSpec{
					ClusterRef: gvdbv1alpha1.LocalObjectReference{Name: clusterName},
					Collection: "products",
					Target:     gvdbv1alpha1.BackupTargetSpec{},
				},
			})).To(Succeed())

			r := &GVDBBackupReconciler{
				Client:    k8sClient,
				Scheme:    k8sClient.Scheme(),
				StatsPool: gvdbclient.NewPool(),
			}
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: backupKey})
			Expect(err).NotTo(HaveOccurred())

			got := &gvdbv1alpha1.GVDBBackup{}
			Expect(k8sClient.Get(ctx, backupKey, got)).To(Succeed())
			Expect(got.Status.Phase).To(Equal(gvdbv1alpha1.BackupPhaseFailed))
			Expect(got.Status.Conditions[0].Reason).To(Equal("InvalidTarget"))
		})

		It("fails fast when the referenced cluster does not exist", func() {
			Expect(k8sClient.Create(ctx, &gvdbv1alpha1.GVDBBackup{
				ObjectMeta: metav1.ObjectMeta{Name: backupName, Namespace: namespace},
				Spec: gvdbv1alpha1.GVDBBackupSpec{
					ClusterRef: gvdbv1alpha1.LocalObjectReference{Name: "missing-cluster"},
					Collection: "products",
					Target: gvdbv1alpha1.BackupTargetSpec{
						S3: &gvdbv1alpha1.S3BackupTarget{Bucket: "b"},
					},
				},
			})).To(Succeed())

			r := &GVDBBackupReconciler{
				Client:    k8sClient,
				Scheme:    k8sClient.Scheme(),
				StatsPool: gvdbclient.NewPool(),
			}
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: backupKey})
			Expect(err).NotTo(HaveOccurred())

			got := &gvdbv1alpha1.GVDBBackup{}
			Expect(k8sClient.Get(ctx, backupKey, got)).To(Succeed())
			Expect(got.Status.Phase).To(Equal(gvdbv1alpha1.BackupPhaseFailed))
			Expect(got.Status.Conditions[0].Reason).To(Equal("ClusterNotFound"))
		})

		It("is a no-op once the CR is in a terminal phase", func() {
			Expect(k8sClient.Create(ctx, &gvdbv1alpha1.GVDBCluster{
				ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
			})).To(Succeed())
			b := &gvdbv1alpha1.GVDBBackup{
				ObjectMeta: metav1.ObjectMeta{Name: backupName, Namespace: namespace},
				Spec: gvdbv1alpha1.GVDBBackupSpec{
					ClusterRef: gvdbv1alpha1.LocalObjectReference{Name: clusterName},
					Collection: "products",
					Target: gvdbv1alpha1.BackupTargetSpec{
						S3: &gvdbv1alpha1.S3BackupTarget{Bucket: "b"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, b)).To(Succeed())
			// Mark the CR terminal directly via the status subresource so
			// subsequent reconciles short-circuit.
			b.Status.Phase = gvdbv1alpha1.BackupPhaseCompleted
			b.Status.BackupID = "bk-existing"
			Expect(k8sClient.Status().Update(ctx, b)).To(Succeed())

			r := &GVDBBackupReconciler{
				Client:    k8sClient,
				Scheme:    k8sClient.Scheme(),
				StatsPool: gvdbclient.NewPool(),
			}
			result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: backupKey})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())

			got := &gvdbv1alpha1.GVDBBackup{}
			Expect(k8sClient.Get(ctx, backupKey, got)).To(Succeed())
			// Phase must not flip — the no-op path must NOT clobber the
			// terminal status the test wrote.
			Expect(got.Status.Phase).To(Equal(gvdbv1alpha1.BackupPhaseCompleted))
			Expect(got.Status.BackupID).To(Equal("bk-existing"))
		})
	})
})
