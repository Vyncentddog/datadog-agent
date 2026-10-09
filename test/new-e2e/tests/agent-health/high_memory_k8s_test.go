// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2026-present Datadog, Inc.

package agenthealth

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"

	"github.com/DataDog/agent-payload/v5/healthplatform"

	"github.com/DataDog/datadog-agent/test/e2e-framework/components/datadog/kubernetesagentparams"
	scenariokindvm "github.com/DataDog/datadog-agent/test/e2e-framework/scenarios/aws/kindvm"
	"github.com/DataDog/datadog-agent/test/e2e-framework/testing/e2e"
	"github.com/DataDog/datadog-agent/test/e2e-framework/testing/environments"
	provkindvm "github.com/DataDog/datadog-agent/test/e2e-framework/testing/provisioners/aws/kubernetes/kindvm"
)

const (
	// highMemoryK8sIssuePrefix is the base IssueID of comp/healthplatform/issues/highmemoryk8s; the full id is
	// suffixed with a hash of the Agent's discriminator.
	highMemoryK8sIssuePrefix = "high-memory-k8s:"

	nodeAgentNamespace = "datadog"
	nodeAgentContainer = "agent"

	// highMemoryK8sDetectionTimeout covers the 5 minute evaluation window filling with high samples, the 1 minute
	// evaluation interval and the 30s forwarder interval.
	highMemoryK8sDetectionTimeout = 10 * time.Minute
	// highMemoryK8sResolutionTimeout covers one low sample pulling the 5 minute average under the threshold.
	highMemoryK8sResolutionTimeout = 5 * time.Minute
)

type highMemoryK8sSuite struct {
	e2e.BaseSuite[environments.Kubernetes]
}

func TestHighMemoryK8sSuite(t *testing.T) {
	t.Parallel()
	e2e.Run(t, &highMemoryK8sSuite{},
		e2e.WithProvisioner(provkindvm.Provisioner(
			provkindvm.WithRunOptions(
				scenariokindvm.WithAgentOptions(
					kubernetesagentparams.WithHelmValues(highMemoryK8sHelmValues),
				),
			),
		)),
	)
}

// TestHighMemoryK8sIssueLifecycle verifies that holding the node Agent container above 90% of its memory limit is
// reported to fakeintake as ACTIVE, and that releasing the memory reports it as RESOLVED.
//
// Cross-restart persistence is tested separately in TestResilienceSuite.
func (suite *highMemoryK8sSuite) TestHighMemoryK8sIssueLifecycle() {
	fakeIntake := suite.Env().FakeIntake.Client()
	pod := suite.nodeAgentPod()

	// Leave the Agent container as provisioned so the test can be retried on the same infra.
	suite.T().Cleanup(func() {
		_, _, _ = suite.execInNodeAgent(pod, stopMemHogCommand)
	})

	suite.T().Run("IssueDetection", func(t *testing.T) {
		_, stderr, err := suite.execInNodeAgent(pod, startMemHogCommand)
		require.NoError(t, err, "failed to start the memory allocation: %s", stderr)

		reached := assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			_, _, err := suite.execInNodeAgent(pod, "test -f "+memHogPIDFile)
			assert.NoError(ct, err, "memory allocation has not reached its target yet")
		}, 2*time.Minute, 5*time.Second, "memory allocation did not reach its target")
		if !reached {
			t.Fatalf("memory diagnostics: %s", suite.memHogDiagnostics(pod))
		}

		var issues []*healthplatform.Issue
		detected := assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			payloads, err := fakeIntake.GetAgentHealth()
			assert.NoError(ct, err)
			issues = nil
			for _, p := range payloads {
				for _, iss := range findIssuesByPrefix(p, highMemoryK8sIssuePrefix) {
					if iss.PersistedIssue != nil && iss.PersistedIssue.State == healthplatform.IssueState_ISSUE_STATE_ACTIVE {
						issues = append(issues, iss)
					}
				}
			}
			assert.NotEmpty(ct, issues, "issue not found as ACTIVE in fakeintake")
		}, highMemoryK8sDetectionTimeout, defaultIssuePollInterval, "issue not detected as ACTIVE in fakeintake")
		if !detected {
			t.Fatalf("memory diagnostics: %s", suite.memHogDiagnostics(pod))
		}

		require.NotEmpty(t, issues)
		issue := issues[0]
		assert.Equal(t, "High Memory K8s", issue.IssueName)
		assert.Equal(t, "high_memory_k8s", issue.IssueType)
		assert.Equal(t, "resources", issue.Category)
		assert.Equal(t, "agent", issue.Source)
		assert.Equal(t, "agent", issue.Location)
		assert.Equal(t, healthplatform.IssueSeverity_ISSUE_SEVERITY_HIGH, issue.Severity)
		assert.Contains(t, issue.Tags, "memory")
		assert.Contains(t, issue.Tags, "kubernetes")
		require.NotNil(t, issue.Remediation)
		assert.NotEmpty(t, issue.Remediation.Summary)
		assert.NotEmpty(t, issue.Remediation.Steps)
	})

	suite.T().Run("Resolution", func(t *testing.T) {
		_, stderr, err := suite.execInNodeAgent(pod, stopMemHogCommand)
		require.NoError(t, err, "failed to release the memory allocation: %s", stderr)
		require.NoError(t, fakeIntake.FlushServerAndResetAggregators())

		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			payloads, err := fakeIntake.GetAgentHealth()
			assert.NoError(ct, err)
			var resolved bool
			for _, p := range payloads {
				for _, iss := range findIssuesByPrefix(p, highMemoryK8sIssuePrefix) {
					if iss.PersistedIssue != nil && iss.PersistedIssue.State == healthplatform.IssueState_ISSUE_STATE_RESOLVED {
						resolved = true
					}
				}
			}
			assert.True(ct, resolved, "issue not found as RESOLVED in fakeintake")
		}, highMemoryK8sResolutionTimeout, defaultIssuePollInterval, "issue not resolved after releasing the memory")
	})
}

// nodeAgentPod returns the name of the node Agent pod. The kind cluster has a single node, so there is one.
func (suite *highMemoryK8sSuite) nodeAgentPod() string {
	pods, err := suite.Env().KubernetesCluster.Client().CoreV1().Pods(nodeAgentNamespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: fields.OneTermEqualSelector("app", suite.Env().Agent.LinuxNodeAgent.LabelSelectors["app"]).String(),
	})
	require.NoError(suite.T(), err)
	require.Len(suite.T(), pods.Items, 1, "expected exactly one node Agent pod")
	return pods.Items[0].Name
}

// execInNodeAgent runs cmd with sh -c in the node Agent container.
func (suite *highMemoryK8sSuite) execInNodeAgent(pod, cmd string) (stdout, stderr string, err error) {
	return suite.Env().KubernetesCluster.KubernetesClient.PodExec(nodeAgentNamespace, pod, nodeAgentContainer, []string{"sh", "-c", cmd})
}

// memHogDiagnostics returns the memory allocation output and the container memory accounting, to tell apart an
// allocation that failed from an issue that was not reported.
func (suite *highMemoryK8sSuite) memHogDiagnostics(pod string) string {
	stdout, stderr, err := suite.execInNodeAgent(pod, "cat "+memHogLogFile+"; echo; "+
		"echo memory.max: $(cat /sys/fs/cgroup/memory.max); "+
		"echo memory.current: $(cat /sys/fs/cgroup/memory.current); "+
		"grep -E '^(anon|inactive_file) ' /sys/fs/cgroup/memory.stat")
	if err != nil {
		return "unable to collect memory diagnostics: " + err.Error() + " " + stderr
	}
	return "\n" + stdout
}
