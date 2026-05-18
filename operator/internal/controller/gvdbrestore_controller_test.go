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

var _ = Describe("GVDBRestore Controller", func() {
	Context("When reconciling a GVDBRestore", func() {
		const restoreName = "test-restore"
		const clusterName = "test-cluster"
		const namespace = "default"

		ctx := context.Background()
		restoreKey := types.NamespacedName{Name: restoreName, Namespace: namespace}
		clusterKey := types.NamespacedName{Name: clusterName, Namespace: namespace}

		AfterEach(func() {
			r := &gvdbv1alpha1.GVDBRestore{}
			if err := k8sClient.Get(ctx, restoreKey, r); err == nil {
				Expect(k8sClient.Delete(ctx, r)).To(Succeed())
			}
			c := &gvdbv1alpha1.GVDBCluster{}
			if err := k8sClient.Get(ctx, clusterKey, c); err == nil {
				Expect(k8sClient.Delete(ctx, c)).To(Succeed())
			}
		})

		It("rejects fromBackupRef and target/backupID set simultaneously", func() {
			Expect(k8sClient.Create(ctx, &gvdbv1alpha1.GVDBCluster{
				ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
			})).To(Succeed())
			Expect(k8sClient.Create(ctx, &gvdbv1alpha1.GVDBRestore{
				ObjectMeta: metav1.ObjectMeta{Name: restoreName, Namespace: namespace},
				Spec: gvdbv1alpha1.GVDBRestoreSpec{
					ClusterRef:    gvdbv1alpha1.LocalObjectReference{Name: clusterName},
					FromBackupRef: &gvdbv1alpha1.LocalObjectReference{Name: "some-backup"},
					BackupID:      "bk-x",
					Target: &gvdbv1alpha1.BackupTargetSpec{
						S3: &gvdbv1alpha1.S3BackupTarget{Bucket: "b"},
					},
				},
			})).To(Succeed())

			rec := &GVDBRestoreReconciler{
				Client:    k8sClient,
				Scheme:    k8sClient.Scheme(),
				StatsPool: gvdbclient.NewPool(),
			}
			_, err := rec.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
			Expect(err).NotTo(HaveOccurred())

			got := &gvdbv1alpha1.GVDBRestore{}
			Expect(k8sClient.Get(ctx, restoreKey, got)).To(Succeed())
			Expect(got.Status.Phase).To(Equal(gvdbv1alpha1.BackupPhaseFailed))
			Expect(got.Status.Conditions[0].Reason).To(Equal("InvalidSource"))
		})

		It("rejects an empty source", func() {
			Expect(k8sClient.Create(ctx, &gvdbv1alpha1.GVDBCluster{
				ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
			})).To(Succeed())
			Expect(k8sClient.Create(ctx, &gvdbv1alpha1.GVDBRestore{
				ObjectMeta: metav1.ObjectMeta{Name: restoreName, Namespace: namespace},
				Spec: gvdbv1alpha1.GVDBRestoreSpec{
					ClusterRef: gvdbv1alpha1.LocalObjectReference{Name: clusterName},
				},
			})).To(Succeed())

			rec := &GVDBRestoreReconciler{
				Client:    k8sClient,
				Scheme:    k8sClient.Scheme(),
				StatsPool: gvdbclient.NewPool(),
			}
			_, err := rec.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
			Expect(err).NotTo(HaveOccurred())

			got := &gvdbv1alpha1.GVDBRestore{}
			Expect(k8sClient.Get(ctx, restoreKey, got)).To(Succeed())
			Expect(got.Status.Phase).To(Equal(gvdbv1alpha1.BackupPhaseFailed))
			Expect(got.Status.Conditions[0].Reason).To(Equal("InvalidSource"))
		})

		It("fails when the referenced GVDBBackup has no allocated id yet", func() {
			Expect(k8sClient.Create(ctx, &gvdbv1alpha1.GVDBCluster{
				ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
			})).To(Succeed())
			// Create a backup CR without a status.backupID (Pending).
			Expect(k8sClient.Create(ctx, &gvdbv1alpha1.GVDBBackup{
				ObjectMeta: metav1.ObjectMeta{Name: "pending-bkp", Namespace: namespace},
				Spec: gvdbv1alpha1.GVDBBackupSpec{
					ClusterRef: gvdbv1alpha1.LocalObjectReference{Name: clusterName},
					Collection: "products",
					Target: gvdbv1alpha1.BackupTargetSpec{
						S3: &gvdbv1alpha1.S3BackupTarget{Bucket: "b"},
					},
				},
			})).To(Succeed())
			Expect(k8sClient.Create(ctx, &gvdbv1alpha1.GVDBRestore{
				ObjectMeta: metav1.ObjectMeta{Name: restoreName, Namespace: namespace},
				Spec: gvdbv1alpha1.GVDBRestoreSpec{
					ClusterRef:    gvdbv1alpha1.LocalObjectReference{Name: clusterName},
					FromBackupRef: &gvdbv1alpha1.LocalObjectReference{Name: "pending-bkp"},
				},
			})).To(Succeed())

			rec := &GVDBRestoreReconciler{
				Client:    k8sClient,
				Scheme:    k8sClient.Scheme(),
				StatsPool: gvdbclient.NewPool(),
			}
			_, err := rec.Reconcile(ctx, reconcile.Request{NamespacedName: restoreKey})
			Expect(err).NotTo(HaveOccurred())

			got := &gvdbv1alpha1.GVDBRestore{}
			Expect(k8sClient.Get(ctx, restoreKey, got)).To(Succeed())
			Expect(got.Status.Phase).To(Equal(gvdbv1alpha1.BackupPhaseFailed))
			Expect(got.Status.Conditions[0].Message).To(ContainSubstring("has no status.backupID"))

			b := &gvdbv1alpha1.GVDBBackup{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: "pending-bkp", Namespace: namespace}, b)).To(Succeed())
			Expect(k8sClient.Delete(ctx, b)).To(Succeed())
		})
	})
})
