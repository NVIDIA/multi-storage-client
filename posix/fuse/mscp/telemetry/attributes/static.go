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

package attributes

import (
	"go.opentelemetry.io/otel/attribute"
)

// `StaticAttributesProvider` provides static attributes from configuration.
// Matches Python: `multistorageclient.telemetry.attributes.static.StaticAttributesProvider`
type StaticAttributesProvider struct {
	attributes []attribute.KeyValue
}

// `NewStaticAttributesProvider` creates a new static attributes provider.
// Options should contain "attributes" key with a map[string]interface{} of static key-value pairs.
// Matches Python: `StaticAttributesProvider.__init__(attributes: api_types.Attributes)`
func NewStaticAttributesProvider(options map[string]interface{}) *StaticAttributesProvider {
	attrs := []attribute.KeyValue{}

	if attrsInterface, ok := options["attributes"]; ok {
		if attrsMap, ok := attrsInterface.(map[string]interface{}); ok {
			for key, value := range attrsMap {
				// Convert value to appropriate attribute type
				switch v := value.(type) {
				case string:
					attrs = append(attrs, attribute.String(key, v))
				case int:
					attrs = append(attrs, attribute.Int(key, v))
				case int64:
					attrs = append(attrs, attribute.Int64(key, v))
				case float64:
					attrs = append(attrs, attribute.Float64(key, v))
				case bool:
					attrs = append(attrs, attribute.Bool(key, v))
				}
			}
		}
	}

	return &StaticAttributesProvider{
		attributes: attrs,
	}
}

// `Attributes` returns the static attributes.
// Matches Python: `StaticAttributesProvider.attributes() -> api_types.Attributes`
func (p *StaticAttributesProvider) Attributes() []attribute.KeyValue {
	// Return a copy to avoid external modification
	result := make([]attribute.KeyValue, len(p.attributes))
	copy(result, p.attributes)
	return result
}
