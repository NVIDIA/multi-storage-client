// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package attributes provides OpenTelemetry attributes providers.
// Matches MSC Python's telemetry.attributes functionality.
package attributes

import (
	"go.opentelemetry.io/otel/attribute"
)

// `AttributesProvider` interface matches Python's `AttributesProvider` base class.
// Each provider implements this interface to contribute attributes to metrics.
type AttributesProvider interface {
	// Attributes returns a slice of OpenTelemetry attributes.
	// Matches Python: def attributes(self) -> api_types.Attributes
	Attributes() []attribute.KeyValue
}

// CollectAttributes merges attributes from multiple providers.
// If multiple providers return an attribute with the same key, the value from the last provider is kept.
// Matches Python: telemetry.attributes.base.collect_attributes()
func CollectAttributes(providers []AttributesProvider) []attribute.KeyValue {
	// Use a map to handle key collisions (last wins)
	attrMap := make(map[string]attribute.KeyValue)

	for _, provider := range providers {
		attrs := provider.Attributes()
		for _, attr := range attrs {
			attrMap[string(attr.Key)] = attr
		}
	}

	// Convert map back to slice
	result := make([]attribute.KeyValue, 0, len(attrMap))
	for _, attr := range attrMap {
		result = append(result, attr)
	}

	return result
}
