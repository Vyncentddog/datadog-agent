// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2025-present Datadog, Inc.

//go:build kubeapiserver && test

package orchestrator

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sdiscovery "k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	core "github.com/DataDog/datadog-agent/pkg/collector/corechecks"
	"github.com/DataDog/datadog-agent/pkg/collector/corechecks/cluster/orchestrator/collectors"
	"github.com/DataDog/datadog-agent/pkg/collector/corechecks/cluster/orchestrator/collectors/inventory"
	"github.com/DataDog/datadog-agent/pkg/collector/corechecks/cluster/orchestrator/collectors/k8s"
	"github.com/DataDog/datadog-agent/pkg/collector/corechecks/cluster/orchestrator/discovery"
	mockconfig "github.com/DataDog/datadog-agent/pkg/config/mock"
	orchcfg "github.com/DataDog/datadog-agent/pkg/orchestrator/config"
	"github.com/DataDog/datadog-agent/pkg/util/kubernetes/apiserver"
)

// createMockAPIClient creates a mock APIClient with fake Kubernetes and dynamic clients
// configured with the necessary custom resource types for testing.
func createMockAPIClient() *apiserver.APIClient {
	client := k8sfake.NewClientset()
	scheme := runtime.NewScheme()

	// Register custom resource types that the tests expect
	dynamicClient := fake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			// Datadog resources
			{Group: "datadoghq.com", Version: "v1alpha1", Resource: "datadogmetrics"}:          "DatadogMetricList",
			{Group: "datadoghq.com", Version: "v1alpha1", Resource: "datadogmonitors"}:         "DatadogMonitorList",
			{Group: "datadoghq.com", Version: "v1alpha1", Resource: "datadogslos"}:             "DatadogSloList",
			{Group: "datadoghq.com", Version: "v1alpha1", Resource: "datadogdashboards"}:       "DatadogDashboardList",
			{Group: "datadoghq.com", Version: "v1alpha1", Resource: "datadogagentprofiles"}:    "DatadogAgentProfileList",
			{Group: "datadoghq.com", Version: "v1alpha1", Resource: "datadogpodautoscalers"}:   "DatadogPodAutoscalerList",
			{Group: "datadoghq.com", Version: "v1alpha2", Resource: "datadogpodautoscalers"}:   "DatadogPodAutoscalerList",
			{Group: "datadoghq.com", Version: "v2alpha1", Resource: "datadogagents"}:           "DatadogAgentList",
			{Group: "datadoghq.com", Version: "v1alpha1", Resource: "datadoginstrumentations"}: "DatadogInstrumentationList",
			// Third-party resources
			{Group: "argoproj.io", Version: "v1alpha1", Resource: "rollouts"}: "RolloutList",
		})

	return &apiserver.APIClient{
		Cl:        client,
		DynamicCl: dynamicClient,
	}
}

func TestImportBuiltinCollectors(t *testing.T) {
	cfg := mockconfig.New(t)
	cfg.SetInTest("orchestrator_explorer.terminated_pods.enabled", true)
	cfg.SetInTest("orchestrator_explorer.custom_resources.ootb.enabled", true)

	// Set up discovery cache with supported resources
	collectorDiscovery := &discovery.DiscoveryCollector{}
	collectorDiscovery.SetCache(discovery.DiscoveryCache{
		CollectorForVersion: map[discovery.CollectorVersion]struct{}{
			{GroupVersion: "v1", Kind: "pods"}:                                        {},
			{GroupVersion: "datadoghq.com/v1alpha1", Kind: "datadogmetrics"}:          {},
			{GroupVersion: "datadoghq.com/v1alpha1", Kind: "datadogmonitors"}:         {},
			{GroupVersion: "datadoghq.com/v1alpha1", Kind: "datadogslos"}:             {},
			{GroupVersion: "datadoghq.com/v1alpha1", Kind: "datadogdashboards"}:       {},
			{GroupVersion: "datadoghq.com/v1alpha1", Kind: "datadogagentprofiles"}:    {},
			{GroupVersion: "datadoghq.com/v1alpha1", Kind: "datadogpodautoscalers"}:   {},
			{GroupVersion: "datadoghq.com/v1alpha2", Kind: "datadogpodautoscalers"}:   {},
			{GroupVersion: "datadoghq.com/v2alpha1", Kind: "datadogagents"}:           {},
			{GroupVersion: "datadoghq.com/v1alpha1", Kind: "datadoginstrumentations"}: {},
		},
		Groups: []*v1.APIGroup{
			{
				Name: "datadoghq.com",
				Versions: []v1.GroupVersionForDiscovery{
					{Version: "v1alpha1"},
					{Version: "v1alpha2"},
					{Version: "v2alpha1"},
				},
				PreferredVersion: v1.GroupVersionForDiscovery{
					GroupVersion: "datadoghq.com/v2alpha1",
					Version:      "v2alpha1",
				},
			},
		},
	})

	cb := CollectorBundle{
		collectorDiscovery:  collectorDiscovery,
		activatedCollectors: make(map[string]struct{}),
		collectors: []collectors.K8sCollector{
			k8s.NewUnassignedPodCollector(nil, nil, nil),
			k8s.NewCRDCollector(),
		},
		inventory: inventory.NewCollectorInventory(cfg, nil, nil),
		runCfg: &collectors.CollectorRunConfig{
			K8sCollectorRunConfig: collectors.K8sCollectorRunConfig{
				APIClient: createMockAPIClient(),
			},
		},
	}

	cb.importBuiltinCollectors()
	names := make([]string, 0, len(cb.collectors))
	for _, collector := range cb.collectors {
		names = append(names, collector.Metadata().FullName())
	}

	expected := []string{
		"v1/pods",
		"v1/terminated-pods",
		"apiextensions.k8s.io/v1/customresourcedefinitions",
		"datadoghq.com/v1alpha1/datadogmetrics",
		"datadoghq.com/v1alpha1/datadogmonitors",
		"datadoghq.com/v1alpha1/datadogslos",
		"datadoghq.com/v1alpha1/datadogdashboards",
		"datadoghq.com/v1alpha1/datadogagentprofiles",
		"datadoghq.com/v1alpha2/datadogpodautoscalers", // preferred version selected
		"datadoghq.com/v2alpha1/datadogagents",
		"datadoghq.com/v1alpha1/datadoginstrumentations",
	}
	require.ElementsMatch(t, expected, names)
}

func TestGetDatadogCustomResourceCollectors(t *testing.T) {
	collectorDiscovery := &discovery.DiscoveryCollector{}
	cb := CollectorBundle{
		check: &OrchestratorCheck{
			orchestratorConfig: &orchcfg.OrchestratorConfig{},
		},
		collectors:         []collectors.K8sCollector{},
		collectorDiscovery: collectorDiscovery,
		inventory:          inventory.NewCollectorInventory(mockconfig.New(t), nil, nil),
		runCfg: &collectors.CollectorRunConfig{
			K8sCollectorRunConfig: collectors.K8sCollectorRunConfig{
				APIClient: createMockAPIClient(),
			},
		},
	}

	for _, testCase := range []struct {
		name               string
		enabled            bool
		hasCrdCollectors   bool
		supportedResources discovery.DiscoveryCache
		expected           []string
	}{
		{
			name:             "Datadog CR collectors enabled with all supported resources",
			enabled:          true,
			hasCrdCollectors: true,
			supportedResources: discovery.DiscoveryCache{
				CollectorForVersion: map[discovery.CollectorVersion]struct{}{
					{GroupVersion: "datadoghq.com/v1alpha1", Kind: "datadogmetrics"}:        {},
					{GroupVersion: "datadoghq.com/v1alpha1", Kind: "datadogmonitors"}:       {},
					{GroupVersion: "datadoghq.com/v1alpha1", Kind: "datadogslos"}:           {},
					{GroupVersion: "datadoghq.com/v1alpha1", Kind: "datadogdashboards"}:     {},
					{GroupVersion: "datadoghq.com/v1alpha1", Kind: "datadogagentprofiles"}:  {},
					{GroupVersion: "datadoghq.com/v1alpha1", Kind: "datadogpodautoscalers"}: {},
					{GroupVersion: "datadoghq.com/v1alpha2", Kind: "datadogpodautoscalers"}: {},
					{GroupVersion: "datadoghq.com/v2alpha1", Kind: "datadogagents"}:         {},
				},
				Groups: []*v1.APIGroup{
					{
						Name: "datadoghq.com",
						Versions: []v1.GroupVersionForDiscovery{
							{Version: "v1alpha1"},
							{Version: "v1alpha2"},
							{Version: "v2alpha1"},
						},
						PreferredVersion: v1.GroupVersionForDiscovery{
							GroupVersion: "datadoghq.com/v1alpha2",
							Version:      "v1alpha2",
						},
					},
				},
			},
			expected: []string{
				"datadoghq.com/v1alpha1/datadogmetrics",
				"datadoghq.com/v1alpha1/datadogmonitors",
				"datadoghq.com/v1alpha1/datadogslos",
				"datadoghq.com/v1alpha1/datadogdashboards",
				"datadoghq.com/v1alpha1/datadogagentprofiles",
				"datadoghq.com/v1alpha2/datadogpodautoscalers", // preferred version selected
				"datadoghq.com/v2alpha1/datadogagents",
			},
		},
		{
			name:             "Datadog CR collectors enabled with no supported resources",
			enabled:          true,
			hasCrdCollectors: true,
			supportedResources: discovery.DiscoveryCache{
				CollectorForVersion: map[discovery.CollectorVersion]struct{}{},
				Groups:              []*v1.APIGroup{},
			},
			expected: []string{},
		},
		{
			name:             "Datadog CR collectors enabled with partial supported resources",
			enabled:          true,
			hasCrdCollectors: true,
			supportedResources: discovery.DiscoveryCache{
				CollectorForVersion: map[discovery.CollectorVersion]struct{}{
					{GroupVersion: "datadoghq.com/v1alpha1", Kind: "datadogmetrics"}:  {},
					{GroupVersion: "datadoghq.com/v1alpha1", Kind: "datadogmonitors"}: {},
				},
				Groups: []*v1.APIGroup{
					{
						Name: "datadoghq.com",
						Versions: []v1.GroupVersionForDiscovery{
							{Version: "v1alpha1"},
						},
						PreferredVersion: v1.GroupVersionForDiscovery{
							GroupVersion: "datadoghq.com/v1alpha1",
							Version:      "v1alpha1",
						},
					},
				},
			},
			expected: []string{
				"datadoghq.com/v1alpha1/datadogmetrics",
				"datadoghq.com/v1alpha1/datadogmonitors",
			},
		},
		{
			name:             "Datadog CR collectors disabled",
			enabled:          false,
			hasCrdCollectors: true,
			supportedResources: discovery.DiscoveryCache{
				CollectorForVersion: map[discovery.CollectorVersion]struct{}{
					{GroupVersion: "datadoghq.com/v1alpha1", Kind: "datadogmetrics"}:        {},
					{GroupVersion: "datadoghq.com/v1alpha1", Kind: "datadogmonitors"}:       {},
					{GroupVersion: "datadoghq.com/v1alpha1", Kind: "datadogpodautoscalers"}: {},
					{GroupVersion: "datadoghq.com/v1alpha2", Kind: "datadogpodautoscalers"}: {},
					{GroupVersion: "datadoghq.com/v2alpha1", Kind: "datadogagents"}:         {},
				},
				Groups: []*v1.APIGroup{
					{
						Name: "datadoghq.com",
						Versions: []v1.GroupVersionForDiscovery{
							{Version: "v1alpha1"},
							{Version: "v1alpha2"},
							{Version: "v2alpha1"},
						},
						PreferredVersion: v1.GroupVersionForDiscovery{
							GroupVersion: "datadoghq.com/v1alpha2",
							Version:      "v1alpha2",
						},
					},
				},
			},
			expected: []string{},
		},
		{
			name:             "Datadog CR collectors enabled without CRD collectors",
			enabled:          true,
			hasCrdCollectors: false,
			supportedResources: discovery.DiscoveryCache{
				CollectorForVersion: map[discovery.CollectorVersion]struct{}{
					{GroupVersion: "datadoghq.com/v1alpha1", Kind: "datadogmetrics"}:        {},
					{GroupVersion: "datadoghq.com/v1alpha1", Kind: "datadogmonitors"}:       {},
					{GroupVersion: "datadoghq.com/v1alpha1", Kind: "datadogpodautoscalers"}: {},
					{GroupVersion: "datadoghq.com/v1alpha2", Kind: "datadogpodautoscalers"}: {},
					{GroupVersion: "datadoghq.com/v2alpha1", Kind: "datadogagents"}:         {},
				},
				Groups: []*v1.APIGroup{
					{
						Name: "datadoghq.com",
						Versions: []v1.GroupVersionForDiscovery{
							{Version: "v1alpha1"},
							{Version: "v1alpha2"},
							{Version: "v2alpha1"},
						},
						PreferredVersion: v1.GroupVersionForDiscovery{
							GroupVersion: "datadoghq.com/v1alpha2",
							Version:      "v1alpha2",
						},
					},
				},
			},
			expected: []string{},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			cfg := mockconfig.New(t)
			cfg.SetInTest("orchestrator_explorer.custom_resources.ootb.enabled", testCase.enabled)

			collectorDiscovery.SetCache(testCase.supportedResources)

			cb.collectors = []collectors.K8sCollector{}
			if testCase.hasCrdCollectors {
				cb.collectors = []collectors.K8sCollector{k8s.NewCRDCollector()}
			}

			crCollectors := cb.getBuiltinCustomResourceCollectors()
			require.Equal(t, len(testCase.expected), len(crCollectors))

			names := make([]string, 0, len(crCollectors))
			for _, collector := range crCollectors {
				names = append(names, collector.Metadata().FullName())
			}
			require.ElementsMatch(t, testCase.expected, names)
		})
	}
}

func TestGetTerminatedPodCollector(t *testing.T) {
	cfg := mockconfig.New(t)

	// add pod to discovery cache to ensure that pod collector is supported
	collectorDiscovery := &discovery.DiscoveryCollector{}
	collectorDiscovery.SetCache(
		discovery.DiscoveryCache{
			CollectorForVersion: map[discovery.CollectorVersion]struct{}{
				{GroupVersion: "v1", Kind: "pods"}: {},
			},
		})

	for _, testCase := range []struct {
		name                          string
		terminatedPodsEnabled         bool
		terminatedPodsImprovedEnabled bool
		unassignedPod                 bool
		expected                      collectors.K8sCollector
	}{
		{
			name:                          "Terminated pods collector enabled",
			terminatedPodsEnabled:         true,
			terminatedPodsImprovedEnabled: false,
			unassignedPod:                 true,
			expected:                      k8s.NewTerminatedPodCollector(nil, nil, nil),
		},
		{
			name:                          "Terminated pods improved collector disabled by parent flag",
			terminatedPodsEnabled:         false,
			terminatedPodsImprovedEnabled: true,
			unassignedPod:                 true,
			expected:                      nil,
		},
		{
			name:                          "Terminated pods improved collector takes precedence",
			terminatedPodsEnabled:         true,
			terminatedPodsImprovedEnabled: true,
			unassignedPod:                 true,
			expected:                      k8s.NewImprovedTerminatedPodCollector(nil, nil, nil),
		},
		{
			name:                          "Terminated pods collector disabled",
			terminatedPodsEnabled:         false,
			terminatedPodsImprovedEnabled: false,
			unassignedPod:                 true,
			expected:                      nil,
		},
		{
			name:                          "Terminated pods collector enabled without unassigned pod collector",
			terminatedPodsEnabled:         true,
			terminatedPodsImprovedEnabled: false,
			unassignedPod:                 false,
			expected:                      nil,
		},
		{
			name:                          "Terminated pods improved collector enabled without unassigned pod collector",
			terminatedPodsEnabled:         true,
			terminatedPodsImprovedEnabled: true,
			unassignedPod:                 false,
			expected:                      nil,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			cfg.SetInTest("orchestrator_explorer.terminated_pods.enabled", testCase.terminatedPodsEnabled)
			cfg.SetInTest("orchestrator_explorer.terminated_pods_improved.enabled", testCase.terminatedPodsImprovedEnabled)

			cb := CollectorBundle{
				collectors:         []collectors.K8sCollector{},
				collectorDiscovery: collectorDiscovery,
				inventory:          inventory.NewCollectorInventory(cfg, nil, nil),
			}

			if testCase.unassignedPod {
				cb.collectors = []collectors.K8sCollector{k8s.NewUnassignedPodCollector(nil, nil, nil)}
			}

			collector := cb.getTerminatedPodCollector()
			if testCase.expected == nil {
				require.Nil(t, collector)
			} else {
				require.Equal(t, testCase.expected.Metadata().FullName(), collector.Metadata().FullName())
			}
		})
	}
}

func TestNewBuiltinCRDConfigs(t *testing.T) {
	configs := newBuiltinCRDConfigs()

	// Expected configurations (group/version/kind)
	expectedConfigs := []string{
		// Datadog resources
		"datadoghq.com/v1alpha2/datadogpodautoscalers",
		"datadoghq.com/v1alpha2/datadogpodautoscalerclusterprofiles",
		"datadoghq.com/v2alpha1/datadogagents",
		"datadoghq.com/v1alpha1/datadogslos",
		"datadoghq.com/v1alpha1/datadogdashboards",
		"datadoghq.com/v1alpha1/datadogagentprofiles",
		"datadoghq.com/v1alpha1/datadogmonitors",
		"datadoghq.com/v1alpha1/datadogmetrics",
		"datadoghq.com/v1alpha1/datadoginstrumentations",

		// Argo
		"argoproj.io/v1alpha1/rollouts",
		"argoproj.io/v1alpha1/applications",
		"argoproj.io/v1alpha1/applicationsets",

		// Flux
		"source.toolkit.fluxcd.io/v1/buckets",
		"source.toolkit.fluxcd.io/v1/helmcharts",
		"source.toolkit.fluxcd.io/v1/externalartifacts",
		"source.toolkit.fluxcd.io/v1/gitrepositories",
		"source.toolkit.fluxcd.io/v1/helmrepositories",
		"source.toolkit.fluxcd.io/v1/ocirepositories",
		"kustomize.toolkit.fluxcd.io/v1/kustomizations",

		// karpenter all resources
		"karpenter.sh/v1/",
		"karpenter.k8s.aws/v1/",
		"karpenter.azure.com/v1beta1/",

		// EKS Auto Mode NodeClass resource
		"eks.amazonaws.com/v1/nodeclasses",

		// Dynamo
		"nvidia.com/v1alpha1/dynamocheckpoints",
		"nvidia.com/v1beta1/dynamocomponentdeployments",
		"nvidia.com/v1beta1/dynamographdeploymentrequests",
		"nvidia.com/v1beta1/dynamographdeployments",
		"nvidia.com/v1beta1/dynamographdeploymentscalingadapters",
		"nvidia.com/v1alpha1/dynamomodels",
		"nvidia.com/v1alpha1/dynamoworkermetadatas",

		// KubeRay
		"ray.io/v1/rayclusters",
		"ray.io/v1/raycronjobs",
		"ray.io/v1/rayjobs",
		"ray.io/v1/rayservices",

		// KubeAI
		"kubeai.org/v1/models",

		// KServe
		"serving.kserve.io/v1alpha1/clusterstoragecontainers",
		"serving.kserve.io/v1alpha2/llminferenceserviceconfigs",
		"serving.kserve.io/v1alpha2/llminferenceservices",
		"serving.kserve.io/v1alpha1/localmodelcaches",
		"serving.kserve.io/v1alpha1/localmodelnamespacecaches",
		"serving.kserve.io/v1alpha1/localmodelnodegroups",
		"serving.kserve.io/v1alpha1/localmodelnodes",
		"serving.kserve.io/v1alpha1/clusterservingruntimes",
		"serving.kserve.io/v1alpha1/inferencegraphs",
		"serving.kserve.io/v1beta1/inferenceservices",
		"serving.kserve.io/v1alpha1/servingruntimes",
		"serving.kserve.io/v1alpha1/trainedmodels",

		// Gateway API Inference Extension
		"inference.networking.k8s.io/v1/inferencepools",

		// Gateway API
		"gateway.networking.k8s.io/v1/gateways",
		"gateway.networking.k8s.io/v1/httproutes",
		"gateway.networking.k8s.io/v1/grpcroutes",
		"gateway.networking.k8s.io/v1alpha2/tlsroutes",
		"gateway.networking.k8s.io/v1alpha1/listenersets",

		// Service mesh — Istio
		"networking.istio.io/v1/virtualservices",
		"networking.istio.io/v1/gateways",
		"networking.istio.io/v1/destinationrules",
		"networking.istio.io/v1/serviceentries",
		"networking.istio.io/v1/sidecars",

		// Service mesh — other vendors (group-level)
		"gateway.envoyproxy.io/v1alpha1/",
		"traefik.containo.us/v1alpha1/",
		"policy.linkerd.io/v1beta3/",
		"consul.hashicorp.com/v1alpha1/",
		"mesh.consul.hashicorp.com/v2beta1/",
		"kuma.io/v1alpha1/",

		// Ingress controllers — NGINX
		"k8s.nginx.org/v1/virtualservers",
		"k8s.nginx.org/v1/virtualserverroutes",

		// Ingress controllers — Traefik
		"traefik.io/v1alpha1/ingressroutes",

		// Ingress controllers — other vendors (group-level)
		"configuration.konghq.com/v1/",
		"core.haproxy.org/v1alpha2/",
		"ingress.v1.haproxy.org/v1/",
	}

	// Verify all expected configs are present
	foundConfigs := make([]string, 0, len(configs))
	for _, config := range configs {
		gvk := config.group + "/" + config.preferredVersion + "/" + config.kind
		foundConfigs = append(foundConfigs, gvk)

		// Verify config structure
		require.NotEmpty(t, config.group, "Group should not be empty")
		require.NotEmpty(t, config.preferredVersion, "Version should not be empty")
	}

	require.ElementsMatch(t, expectedConfigs, foundConfigs)
}

func TestBuiltinGAIECollector(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		disabled  bool
		version   string
		collected bool
	}{
		{name: "v1 collected by default", version: "v1", collected: true},
		{name: "global OOTB disabled", disabled: true, version: "v1"},
		{name: "API group absent"},
		{name: "unsupported version", version: "v1alpha2"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			cfg := mockconfig.New(t)
			if testCase.disabled {
				cfg.SetInTest("orchestrator_explorer.custom_resources.ootb.enabled", false)
			}
			// GAIE collection does not require opting into the Gateway API family.
			cfg.SetInTest("orchestrator_explorer.custom_resources.ootb.gateway_api", false)

			cache := discovery.DiscoveryCache{
				CollectorForVersion: map[discovery.CollectorVersion]struct{}{},
			}
			if testCase.version != "" {
				cache.CollectorForVersion[discovery.CollectorVersion{
					GroupVersion: GAIEAPIGroup + "/" + testCase.version,
					Kind:         "inferencepools",
				}] = struct{}{}
				cache.Groups = []*v1.APIGroup{{
					Name: GAIEAPIGroup,
					Versions: []v1.GroupVersionForDiscovery{{
						GroupVersion: GAIEAPIGroup + "/" + testCase.version,
						Version:      testCase.version,
					}},
				}}
			}
			collectorDiscovery := &discovery.DiscoveryCollector{}
			collectorDiscovery.SetCache(cache)
			cb := CollectorBundle{collectorDiscovery: collectorDiscovery}
			for _, config := range newBuiltinCRDConfigs() {
				if config.group != GAIEAPIGroup {
					continue
				}
				crCollectors := cb.collectorsForBuiltinCRD(config)
				if testCase.collected {
					require.Len(t, crCollectors, 1)
					require.Equal(t, "inference.networking.k8s.io/v1/inferencepools", crCollectors[0].Metadata().FullName())
				} else {
					require.Empty(t, crCollectors)
				}
			}
		})
	}
}

func TestNewBuiltinKServeCRDConfigs(t *testing.T) {
	cfg := mockconfig.New(t)
	cfg.SetInTest("orchestrator_explorer.custom_resources.ootb.enabled", true)

	var kserveConfigs []builtinCRDConfig
	for _, config := range newBuiltinCRDConfigs() {
		if config.group == KServeAPIGroup {
			kserveConfigs = append(kserveConfigs, config)
		}
	}

	expectedConfigs := []builtinCRDConfig{
		newBuiltinCRDConfig(KServeAPIGroup, "clusterstoragecontainers", true, "v1alpha1"),
		newBuiltinCRDConfig(KServeAPIGroup, "llminferenceserviceconfigs", true, "v1alpha2", "v1alpha1"),
		newBuiltinCRDConfig(KServeAPIGroup, "llminferenceservices", true, "v1alpha2", "v1alpha1"),
		newBuiltinCRDConfig(KServeAPIGroup, "localmodelcaches", true, "v1alpha1"),
		newBuiltinCRDConfig(KServeAPIGroup, "localmodelnamespacecaches", true, "v1alpha1"),
		newBuiltinCRDConfig(KServeAPIGroup, "localmodelnodegroups", true, "v1alpha1"),
		newBuiltinCRDConfig(KServeAPIGroup, "localmodelnodes", true, "v1alpha1"),
		newBuiltinCRDConfig(KServeAPIGroup, "clusterservingruntimes", true, "v1alpha1"),
		newBuiltinCRDConfig(KServeAPIGroup, "inferencegraphs", true, "v1alpha1"),
		newBuiltinCRDConfig(KServeAPIGroup, "inferenceservices", true, "v1beta1"),
		newBuiltinCRDConfig(KServeAPIGroup, "servingruntimes", true, "v1alpha1"),
		newBuiltinCRDConfig(KServeAPIGroup, "trainedmodels", true, "v1alpha1"),
	}

	require.ElementsMatch(t, expectedConfigs, kserveConfigs)
}

func TestNewBuiltinCRDConfigsPerFamilyFlags(t *testing.T) {
	for _, testCase := range []struct {
		name                string
		useDefaults         bool
		ootbEnabled         bool
		gatewayAPI          bool
		serviceMesh         bool
		ingressControllers  bool
		expectedGatewayAPI  int // 5 entries
		expectedServiceMesh int // 11 entries (5 Istio + 6 group-level)
		expectedIngress     int // 6 entries (2 NGINX + 1 Traefik + 3 group-level)
	}{
		{
			name:                "all families enabled by default",
			useDefaults:         true,
			expectedGatewayAPI:  5,
			expectedServiceMesh: 11,
			expectedIngress:     6,
		},
		{
			name:                "all families enabled",
			ootbEnabled:         true,
			gatewayAPI:          true,
			serviceMesh:         true,
			ingressControllers:  true,
			expectedGatewayAPI:  5,
			expectedServiceMesh: 11,
			expectedIngress:     6,
		},
		{
			name:                "gateway_api disabled",
			ootbEnabled:         true,
			gatewayAPI:          false,
			serviceMesh:         true,
			ingressControllers:  true,
			expectedGatewayAPI:  0,
			expectedServiceMesh: 11,
			expectedIngress:     6,
		},
		{
			name:                "service_mesh disabled",
			ootbEnabled:         true,
			gatewayAPI:          true,
			serviceMesh:         false,
			ingressControllers:  true,
			expectedGatewayAPI:  5,
			expectedServiceMesh: 0,
			expectedIngress:     6,
		},
		{
			name:                "ingress_controllers disabled",
			ootbEnabled:         true,
			gatewayAPI:          true,
			serviceMesh:         true,
			ingressControllers:  false,
			expectedGatewayAPI:  5,
			expectedServiceMesh: 11,
			expectedIngress:     0,
		},
		{
			name:                "global ootb disabled overrides all",
			ootbEnabled:         false,
			gatewayAPI:          true,
			serviceMesh:         true,
			ingressControllers:  true,
			expectedGatewayAPI:  0,
			expectedServiceMesh: 0,
			expectedIngress:     0,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			cfg := mockconfig.New(t)
			if !testCase.useDefaults {
				cfg.SetInTest("orchestrator_explorer.custom_resources.ootb.enabled", testCase.ootbEnabled)
				cfg.SetInTest("orchestrator_explorer.custom_resources.ootb.gateway_api", testCase.gatewayAPI)
				cfg.SetInTest("orchestrator_explorer.custom_resources.ootb.service_mesh", testCase.serviceMesh)
				cfg.SetInTest("orchestrator_explorer.custom_resources.ootb.ingress_controllers", testCase.ingressControllers)
			}

			configs := newBuiltinCRDConfigs()

			gatewayAPIGroups := map[string]bool{GatewayAPIGroup: true}
			serviceMeshGroups := map[string]bool{
				IstioNetworkingAPIGroup: true, EnvoyGatewayAPIGroup: true,
				TraefikLegacyAPIGroup: true, LinkerdPolicyAPIGroup: true,
				ConsulAPIGroup: true, ConsulMeshAPIGroup: true, KumaAPIGroup: true,
			}
			ingressGroups := map[string]bool{
				NginxAPIGroup: true, TraefikAPIGroup: true, KongAPIGroup: true,
				HAProxyCoreAPIGroup: true, HAProxyIngressV1APIGroup: true,
			}

			var gatewayCount, meshCount, ingressCount int
			for _, c := range configs {
				if !c.enabled {
					continue
				}
				switch {
				case gatewayAPIGroups[c.group]:
					gatewayCount++
				case serviceMeshGroups[c.group]:
					meshCount++
				case ingressGroups[c.group]:
					ingressCount++
				}
			}

			require.Equal(t, testCase.expectedGatewayAPI, gatewayCount, "gateway API count mismatch")
			require.Equal(t, testCase.expectedServiceMesh, meshCount, "service mesh count mismatch")
			require.Equal(t, testCase.expectedIngress, ingressCount, "ingress controllers count mismatch")
		})
	}
}

func TestFilterCRCollectorsByPermission(t *testing.T) {
	// Create test collectors
	c1, err := k8s.NewCRCollector("datadogmetrics", "datadoghq.com/v1alpha1")
	require.NoError(t, err)
	c2, err := k8s.NewCRCollector("datadogmonitors", "datadoghq.com/v1alpha1")
	require.NoError(t, err)
	c3, err := k8s.NewCRCollector("datadogagents", "datadoghq.com/v1alpha2")
	require.NoError(t, err)

	// Test case 1: All collectors have permissions
	t.Run("All collectors have permissions", func(t *testing.T) {
		permissionMap := map[string]bool{
			"datadoghq.com/v1alpha1/datadogmetrics":  false, // false means NOT forbidden
			"datadoghq.com/v1alpha1/datadogmonitors": false,
			"datadoghq.com/v1alpha2/datadogagents":   false,
		}

		isForbidden := func(gvr schema.GroupVersionResource) bool {
			key := gvr.Group + "/" + gvr.Version + "/" + gvr.Resource
			return permissionMap[key]
		}

		input := []collectors.K8sCollector{c1, c2, c3}
		result := filterCRCollectorsByPermission(input, isForbidden)

		require.Len(t, result, 3)
		names := make([]string, len(result))
		for i, collector := range result {
			names[i] = collector.Metadata().FullName()
		}
		require.ElementsMatch(t, []string{
			"datadoghq.com/v1alpha1/datadogmetrics",
			"datadoghq.com/v1alpha1/datadogmonitors",
			"datadoghq.com/v1alpha2/datadogagents",
		}, names)
	})

	// Test case 2: No collectors have permissions
	t.Run("No collectors have permissions", func(t *testing.T) {
		isForbidden := func(_ schema.GroupVersionResource) bool {
			return true // All resources are forbidden
		}

		input := []collectors.K8sCollector{c1, c2, c3}
		result := filterCRCollectorsByPermission(input, isForbidden)

		require.Len(t, result, 0)
	})

	// Test case 3: Mixed permissions
	t.Run("Mixed permissions", func(t *testing.T) {
		permissionMap := map[string]bool{
			"datadoghq.com/v1alpha1/datadogmetrics":  false, // false means NOT forbidden (allowed)
			"datadoghq.com/v1alpha1/datadogmonitors": true,  // true means forbidden (not allowed)
			"datadoghq.com/v1alpha2/datadogagents":   false,
		}

		isForbidden := func(gvr schema.GroupVersionResource) bool {
			key := gvr.Group + "/" + gvr.Version + "/" + gvr.Resource
			return permissionMap[key]
		}

		input := []collectors.K8sCollector{c1, c2, c3}
		result := filterCRCollectorsByPermission(input, isForbidden)

		require.Len(t, result, 2)
		names := make([]string, len(result))
		for i, collector := range result {
			names[i] = collector.Metadata().FullName()
		}
		require.ElementsMatch(t, []string{
			"datadoghq.com/v1alpha1/datadogmetrics",
			"datadoghq.com/v1alpha2/datadogagents",
		}, names)
	})

	// Test case 4: Empty input
	t.Run("Empty input", func(t *testing.T) {
		isForbidden := func(_ schema.GroupVersionResource) bool {
			return true // All resources are forbidden (doesn't matter for empty input)
		}
		result := filterCRCollectorsByPermission([]collectors.K8sCollector{}, isForbidden)
		require.Len(t, result, 0)
	})
}

func TestCollectorBundleRetryDiscovery(t *testing.T) {
	coreGroup := &v1.APIGroup{Name: "", PreferredVersion: v1.GroupVersionForDiscovery{GroupVersion: "v1", Version: "v1"}}
	appsGroup := &v1.APIGroup{Name: "apps", PreferredVersion: v1.GroupVersionForDiscovery{GroupVersion: "apps/v1", Version: "v1"}}
	crdGroup := &v1.APIGroup{
		Name:             "apiextensions.k8s.io",
		Versions:         []v1.GroupVersionForDiscovery{{GroupVersion: "apiextensions.k8s.io/v1", Version: "v1"}},
		PreferredVersion: v1.GroupVersionForDiscovery{GroupVersion: "apiextensions.k8s.io/v1", Version: "v1"},
	}
	datadogGroup := &v1.APIGroup{
		Name:             "datadoghq.com",
		Versions:         []v1.GroupVersionForDiscovery{{GroupVersion: "datadoghq.com/v1alpha1", Version: "v1alpha1"}},
		PreferredVersion: v1.GroupVersionForDiscovery{GroupVersion: "datadoghq.com/v1alpha1", Version: "v1alpha1"},
	}
	nodesResources := &v1.APIResourceList{GroupVersion: "v1", APIResources: []v1.APIResource{{Name: "nodes"}}}
	coreResources := &v1.APIResourceList{GroupVersion: "v1", APIResources: []v1.APIResource{{Name: "nodes"}, {Name: "services"}}}
	appsResources := &v1.APIResourceList{GroupVersion: "apps/v1", APIResources: []v1.APIResource{{Name: "deployments"}}}
	crdResources := &v1.APIResourceList{GroupVersion: "apiextensions.k8s.io/v1", APIResources: []v1.APIResource{{Name: "customresourcedefinitions"}}}
	datadogResources := &v1.APIResourceList{GroupVersion: "datadoghq.com/v1alpha1", APIResources: []v1.APIResource{{Name: "datadogmetrics"}}}

	failure := retryFetchResult{err: errors.New("client rate limiter Wait returned an error")}
	appsComplete := retryFetchResult{
		groups:    []*v1.APIGroup{coreGroup, appsGroup},
		resources: []*v1.APIResourceList{nodesResources, appsResources},
	}
	appsPartial := retryFetchResult{
		err:       newGroupDiscoveryError(schema.GroupVersion{Group: "apps", Version: "v1"}),
		groups:    []*v1.APIGroup{coreGroup, appsGroup},
		resources: []*v1.APIResourceList{nodesResources},
	}
	coreComplete := retryFetchResult{
		groups:    []*v1.APIGroup{coreGroup},
		resources: []*v1.APIResourceList{coreResources},
	}

	t.Run("Success - Builtin collectors added after partial failure", func(t *testing.T) {
		fetch := newRetryFetch(
			retryFetchResult{
				err:       newGroupDiscoveryError(schema.GroupVersion{Group: "datadoghq.com", Version: "v1alpha1"}),
				groups:    []*v1.APIGroup{crdGroup, datadogGroup},
				resources: []*v1.APIResourceList{crdResources},
			},
			retryFetchResult{
				groups:    []*v1.APIGroup{crdGroup, datadogGroup},
				resources: []*v1.APIResourceList{crdResources, datadogResources},
			},
		)
		settings := map[string]any{"orchestrator_explorer.custom_resources.ootb.enabled": true}
		cb := newRetryTestBundle(t, []string{"customresourcedefinitions"}, false, fetch.fetch, settings)

		cb.prepare()
		require.Equal(t, []string{"apiextensions.k8s.io/v1/customresourcedefinitions"}, collectorNames(cb))

		cb.RetryDiscovery()

		expected := []string{"apiextensions.k8s.io/v1/customresourcedefinitions", "datadoghq.com/v1alpha1/datadogmetrics"}
		require.Equal(t, expected, collectorNames(cb))
		require.Equal(t, 2, fetch.calls)
	})
	t.Run("Success - Collectors added after partial failure", func(t *testing.T) {
		fetch := newRetryFetch(appsPartial, appsComplete)
		cb := newRetryTestBundle(t, []string{"nodes", "deployments"}, false, fetch.fetch, nil)

		cb.prepare()
		require.Equal(t, []string{"v1/nodes"}, collectorNames(cb))
		require.Contains(t, warningMessages(cb), "Kubernetes API discovery failed, retrying in 1 runs: "+appsPartial.err.Error())
		nodesCollector := cb.collectors[0]

		cb.RetryDiscovery()

		require.Equal(t, []string{"v1/nodes", "apps/v1/deployments"}, collectorNames(cb))
		require.Same(t, nodesCollector, cb.collectors[0])
		require.Empty(t, warningMessages(cb))
		require.Equal(t, 2, fetch.calls)
	})
	t.Run("Success - Collectors added after total failure", func(t *testing.T) {
		fetch := newRetryFetch(failure, coreComplete)
		cb := newRetryTestBundle(t, []string{"nodes", "services"}, false, fetch.fetch, nil)

		cb.prepare()
		require.Empty(t, collectorNames(cb))
		require.Equal(t, []string{"Kubernetes API discovery failed, retrying in 1 runs: " + failure.err.Error()}, warningMessages(cb))

		cb.RetryDiscovery()

		require.Equal(t, []string{"v1/nodes", "v1/services"}, collectorNames(cb))
		require.Empty(t, warningMessages(cb))
		require.Equal(t, 2, fetch.calls)
	})
	t.Run("Success - Discovered collectors added after partial failure", func(t *testing.T) {
		fetch := newRetryFetch(appsPartial, appsComplete)
		cb := newRetryTestBundle(t, nil, true, fetch.fetch, nil)

		cb.prepare()
		require.Equal(t, []string{"clusters", "v1/nodes"}, collectorNames(cb))
		previous := slices.Clone(cb.collectors)

		cb.RetryDiscovery()

		require.Equal(t, []string{"clusters", "v1/nodes", "apps/v1/deployments"}, collectorNames(cb))
		require.Same(t, previous[0], cb.collectors[0])
		require.Same(t, previous[1], cb.collectors[1])
	})
	t.Run("Success - No retry for irrelevant partial failure", func(t *testing.T) {
		fetch := newRetryFetch(retryFetchResult{
			err:       newGroupDiscoveryError(schema.GroupVersion{Group: "metrics.k8s.io", Version: "v1beta1"}),
			groups:    []*v1.APIGroup{coreGroup},
			resources: []*v1.APIResourceList{nodesResources},
		})
		cb := newRetryTestBundle(t, []string{"nodes"}, false, fetch.fetch, nil)

		cb.prepare()
		cb.RetryDiscovery()

		require.Equal(t, []string{"v1/nodes"}, collectorNames(cb))
		require.Empty(t, warningMessages(cb))
		require.Equal(t, 1, fetch.calls)
	})
	t.Run("Success - No retry when discovery is complete", func(t *testing.T) {
		fetch := newRetryFetch(coreComplete)
		cb := newRetryTestBundle(t, []string{"nodes", "services"}, false, fetch.fetch, nil)

		cb.prepare()
		cb.RetryDiscovery()

		require.Equal(t, []string{"v1/nodes", "v1/services"}, collectorNames(cb))
		require.Empty(t, warningMessages(cb))
		require.Equal(t, 1, fetch.calls)
	})
	t.Run("Success - Partial failure retried after total failure recovery", func(t *testing.T) {
		// Fails at configure and on the retries of runs 1, 3, 7, 15 and 31, then partially succeeds on run 63.
		results := slices.Repeat([]retryFetchResult{failure}, 6)
		results = append(results, appsPartial, appsComplete)
		fetch := newRetryFetch(results...)
		cb := newRetryTestBundle(t, []string{"nodes", "deployments"}, false, fetch.fetch, nil)

		cb.prepare()
		runRetries(cb, 63)
		require.Equal(t, []string{"v1/nodes"}, collectorNames(cb))
		require.Equal(t, 7, fetch.calls)

		cb.RetryDiscovery()

		require.Equal(t, []string{"v1/nodes", "apps/v1/deployments"}, collectorNames(cb))
		require.Equal(t, 8, fetch.calls)
	})
	t.Run("Success - Partial failure retries back off", func(t *testing.T) {
		fetch := newRetryFetch(appsPartial)
		cb := newRetryTestBundle(t, []string{"nodes", "deployments"}, false, fetch.fetch, nil)

		cb.prepare()
		retryRuns := runRetriesTrackingFetches(cb, fetch, 127)

		require.Equal(t, []int{1, 3, 7, 15, 31, 63, 95, 127}, retryRuns)
		require.Equal(t, []string{"v1/nodes"}, collectorNames(cb))
		_ = warningMessages(cb)

		cb.RetryDiscovery()

		expected := []string{"Kubernetes API discovery is incomplete, retrying in 31 runs: " + appsPartial.err.Error()}
		require.Equal(t, expected, warningMessages(cb))
	})
	t.Run("Success - Total failure retries back off until recovery", func(t *testing.T) {
		// Fails at configure and on the retries of runs 1, 3, 7, 15, 31 and 63, then succeeds on run 95.
		results := slices.Repeat([]retryFetchResult{failure}, 7)
		results = append(results, coreComplete)
		fetch := newRetryFetch(results...)
		cb := newRetryTestBundle(t, nil, true, fetch.fetch, nil)
		stable := make([]string, 0)
		for _, collector := range cb.inventory.StableCollectors() {
			stable = append(stable, collector.Metadata().FullName())
		}

		cb.prepare()
		runRetries(cb, 30)
		require.Empty(t, collectorNames(cb))

		runRetries(cb, 1)
		require.Equal(t, stable, collectorNames(cb))
		require.Equal(t, 6, fetch.calls)

		retryRuns := runRetriesTrackingFetches(cb, fetch, 64)
		require.Equal(t, []int{32, 64}, retryRuns)
		require.Equal(t, stable, collectorNames(cb))

		runRetries(cb, 64)
		require.Equal(t, 8, fetch.calls)
	})
}

func TestCollectorBundleMergeCollectors(t *testing.T) {
	newCRCollector := func(resource, groupVersion string) collectors.K8sCollector {
		collector, err := k8s.NewCRCollectorVersion(resource, groupVersion)
		require.NoError(t, err)
		return collector
	}

	previousWidgetsV1 := newCRCollector("widgets", "example.com/v1")
	previousWidgetsV2 := newCRCollector("widgets", "example.com/v2")
	previousSprockets := newCRCollector("sprockets", "example.org/v1")
	selectedWidgetsV1 := newCRCollector("widgets", "example.com/v1")
	selectedWidgetsV2 := newCRCollector("widgets", "example.com/v2")
	selectedGadgets := newCRCollector("gadgets", "example.com/v1")

	cases := []struct {
		name     string
		previous []collectors.K8sCollector
		selected []collectors.K8sCollector
		expected []collectors.K8sCollector
	}{
		{
			name:     "Success - New collectors added",
			previous: []collectors.K8sCollector{previousSprockets},
			selected: []collectors.K8sCollector{selectedGadgets},
			expected: []collectors.K8sCollector{previousSprockets, selectedGadgets},
		},
		{
			name:     "Success - No previous collector",
			selected: []collectors.K8sCollector{selectedGadgets},
			expected: []collectors.K8sCollector{selectedGadgets},
		},
		{
			name:     "Success - Previous collector kept over another version",
			previous: []collectors.K8sCollector{previousWidgetsV1},
			selected: []collectors.K8sCollector{selectedWidgetsV2, selectedGadgets},
			expected: []collectors.K8sCollector{previousWidgetsV1, selectedGadgets},
		},
		{
			name:     "Success - Previous collector kept when not selected",
			previous: []collectors.K8sCollector{previousSprockets},
			expected: []collectors.K8sCollector{previousSprockets},
		},
		{
			name:     "Success - Previous versions of a resource kept",
			previous: []collectors.K8sCollector{previousWidgetsV1, previousWidgetsV2},
			selected: []collectors.K8sCollector{selectedWidgetsV1, selectedWidgetsV2},
			expected: []collectors.K8sCollector{previousWidgetsV1, previousWidgetsV2},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cb := CollectorBundle{collectors: tt.selected}

			cb.mergeCollectors(tt.previous)

			require.Len(t, cb.collectors, len(tt.expected))
			for i, collector := range tt.expected {
				require.Same(t, collector, cb.collectors[i])
			}
		})
	}
}

func TestCollectorBundleInitialize(t *testing.T) {
	cfg := mockconfig.New(t)
	client := k8sfake.NewClientset()
	apiClient := &apiserver.APIClient{Cl: client, InformerCl: client}
	stopCh := make(chan struct{})
	t.Cleanup(func() { close(stopCh) })

	collectorInventory := inventory.NewCollectorInventory(cfg, nil, nil)
	newCollector := func(name string) collectors.K8sCollector {
		collector, err := collectorInventory.CollectorForDefaultVersion(name)
		require.NoError(t, err)
		return collector
	}

	cb := CollectorBundle{
		collectors:            []collectors.K8sCollector{newCollector("clusters"), newCollector("nodes")},
		initializedCollectors: map[string]struct{}{},
		runCfg: &collectors.CollectorRunConfig{
			K8sCollectorRunConfig: collectors.K8sCollectorRunConfig{
				APIClient:                   apiClient,
				OrchestratorInformerFactory: getOrchestratorInformerFactory(apiClient),
			},
			Config: &orchcfg.OrchestratorConfig{},
		},
		startedInformers: map[cache.SharedInformer]struct{}{},
		stopCh:           stopCh,
	}

	cb.Initialize()
	require.Len(t, cb.startedInformers, 1)

	cb.collectors = append(cb.collectors, newCollector("services"))
	cb.Initialize()

	expectedInitialized := map[string]struct{}{
		"clusters":    {},
		"v1/nodes":    {},
		"v1/services": {},
	}
	require.Equal(t, expectedInitialized, cb.initializedCollectors)
	require.Len(t, cb.startedInformers, 2)
}

func TestCollectorBundleResumeSyncedCollectors(t *testing.T) {
	newBundle := func(t *testing.T) (*CollectorBundle, collectors.K8sCollector) {
		cfg := mockconfig.New(t)
		client := k8sfake.NewClientset()
		apiClient := &apiserver.APIClient{Cl: client, InformerCl: client}
		runCfg := &collectors.CollectorRunConfig{
			K8sCollectorRunConfig: collectors.K8sCollectorRunConfig{
				APIClient:                   apiClient,
				OrchestratorInformerFactory: getOrchestratorInformerFactory(apiClient),
			},
			Config: &orchcfg.OrchestratorConfig{},
		}

		collector, err := inventory.NewCollectorInventory(cfg, nil, nil).CollectorForDefaultVersion("services")
		require.NoError(t, err)
		collector.Init(runCfg)
		collector.Metadata().IsSkipped = true
		collector.Metadata().SkippedReason = "couldn't sync informer v1/services"

		return &CollectorBundle{collectors: []collectors.K8sCollector{collector}}, collector
	}

	t.Run("Success - Synced collector resumed", func(t *testing.T) {
		cb, collector := newBundle(t)
		stopCh := make(chan struct{})
		t.Cleanup(func() { close(stopCh) })
		go collector.Informer().Run(stopCh)

		resumed := func() bool {
			cb.resumeSyncedCollectors()
			return !collector.Metadata().IsSkipped
		}
		require.Eventually(t, resumed, 5*time.Second, 10*time.Millisecond)
		require.Empty(t, collector.Metadata().SkippedReason)
	})
	t.Run("Success - Unsynced collector kept skipped", func(t *testing.T) {
		cb, collector := newBundle(t)

		cb.resumeSyncedCollectors()

		require.True(t, collector.Metadata().IsSkipped)
		require.Equal(t, "couldn't sync informer v1/services", collector.Metadata().SkippedReason)
	})
}

type retryFetch struct {
	calls   int
	results []retryFetchResult
}

// newRetryFetch returns a fetch function returning results in order, the last one being repeated.
func newRetryFetch(results ...retryFetchResult) *retryFetch {
	return &retryFetch{results: results}
}

func (f *retryFetch) fetch() ([]*v1.APIGroup, []*v1.APIResourceList, error) {
	result := f.results[min(f.calls, len(f.results)-1)]
	f.calls++
	return result.groups, result.resources, result.err
}

type retryFetchResult struct {
	err       error
	groups    []*v1.APIGroup
	resources []*v1.APIResourceList
}

func collectorNames(cb *CollectorBundle) []string {
	names := make([]string, 0, len(cb.collectors))
	for _, collector := range cb.collectors {
		names = append(names, collector.Metadata().FullName())
	}
	return names
}

func newGroupDiscoveryError(groupVersions ...schema.GroupVersion) error {
	groups := make(map[schema.GroupVersion]error, len(groupVersions))
	for _, gv := range groupVersions {
		groups[gv] = errors.New("service unavailable")
	}
	return &k8sdiscovery.ErrGroupDiscoveryFailed{Groups: groups}
}

func newRetryTestBundle(t *testing.T, configured []string, discoverCollectors bool, fetch discovery.ServerGroupsAndResourcesFunc, settings map[string]any) *CollectorBundle {
	cfg := mockconfig.New(t)
	for key, value := range settings {
		cfg.SetInTest(key, value)
	}

	return &CollectorBundle{
		activatedCollectors: map[string]struct{}{},
		check: &OrchestratorCheck{
			CheckBase: core.NewCheckBase(CheckName),
			instance:  &OrchestratorInstance{Collectors: configured},
		},
		collectorDiscovery: discovery.NewDiscoveryCollector(fetch),
		discoverCollectors: discoverCollectors,
		discoveryRetrier:   newRetrier(discoveryRetryBaseRuns, discoveryRetryMaxRuns),
		inventory:          inventory.NewCollectorInventory(cfg, nil, nil),
		runCfg: &collectors.CollectorRunConfig{
			K8sCollectorRunConfig: collectors.K8sCollectorRunConfig{
				APIClient: createMockAPIClient(),
			},
		},
	}
}

func runRetries(cb *CollectorBundle, count int) {
	for range count {
		cb.RetryDiscovery()
	}
}

// runRetriesTrackingFetches runs count retries and returns the runs, counted from 1, that fetched the discovery.
func runRetriesTrackingFetches(cb *CollectorBundle, fetch *retryFetch, count int) []int {
	var runs []int
	for run := 1; run <= count; run++ {
		calls := fetch.calls
		cb.RetryDiscovery()
		if fetch.calls > calls {
			runs = append(runs, run)
		}
	}
	return runs
}

// warningMessages returns the check warnings emitted since the last call.
func warningMessages(cb *CollectorBundle) []string {
	warnings := cb.check.GetWarnings()
	messages := make([]string, 0, len(warnings))
	for _, warning := range warnings {
		messages = append(messages, warning.Error())
	}
	return messages
}
