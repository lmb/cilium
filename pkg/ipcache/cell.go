// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package ipcache

import (
	"context"

	"github.com/cilium/cilium/pkg/hive"
	"github.com/cilium/cilium/pkg/hive/cell"
	"github.com/cilium/cilium/pkg/identity/cache"
	ipcacheTypes "github.com/cilium/cilium/pkg/ipcache/types"
	"github.com/cilium/cilium/pkg/policy"
	"github.com/cilium/cilium/pkg/promise"
)

var Cell = cell.Module(
	"ipcache",
	"IPCache maps IPs to identities",

	cell.Provide(newIPCache),
)

func newIPCache(
	lifecycle hive.Lifecycle,
	identityAllocatorPromise promise.Promise[cache.IdentityAllocator],
	policyRepoPromise promise.Promise[*policy.Repository],
	datapathHandler ipcacheTypes.DatapathHandler,
) *IPCache {
	ctx, cancel := context.WithCancel(context.Background())

	ipcache := NewIPCache(&Configuration{
		Context:         ctx,
		DatapathHandler: datapathHandler,
	})

	lifecycle.Append(hive.Hook{
		OnStart: func(hc hive.HookContext) error {
			policyRepo, err := policyRepoPromise.Await(hc)
			if err != nil {
				return err
			}

			if policyRepo != nil {
				ipcache.PolicyHandler = policyRepo.GetSelectorCache()
			}

			ipcache.IdentityAllocator, err = identityAllocatorPromise.Await(hc)
			if err != nil {
				return err
			}

			return nil
		},
		OnStop: func(hc hive.HookContext) error {
			cancel()
			return ipcache.Shutdown()
		},
	})

	return ipcache
}
