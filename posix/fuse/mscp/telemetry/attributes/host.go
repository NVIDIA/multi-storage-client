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
	"os"

	"go.opentelemetry.io/otel/attribute"
)

// `HostAttributesProvider` provides attributes from host information.
// Matches Python: `multistorageclient.telemetry.attributes.host.HostAttributesProvider`
type HostAttributesProvider struct {
	// Map of attribute key to host attribute type
	attributes map[string]string
}

// `NewHostAttributesProvider` creates a new host attributes provider.
// Options should contain "attributes" key with a map[string]string mapping attribute keys to host attributes.
// Supported host attributes: "name" (hostname)
// Matches Python: `HostAttributesProvider.__init__(attributes: Mapping[str, str])`
func NewHostAttributesProvider(options map[string]interface{}) *HostAttributesProvider {
	attrs := make(map[string]string)

	if attrsInterface, ok := options["attributes"]; ok {
		if attrsMap, ok := attrsInterface.(map[string]interface{}); ok {
			for key, value := range attrsMap {
				if strValue, ok := value.(string); ok {
					attrs[key] = strValue
				}
			}
		}
	}

	return &HostAttributesProvider{
		attributes: attrs,
	}
}

// `Attributes` returns attributes collected from host information.
// Matches Python: `HostAttributesProvider.attributes() -> api_types.Attributes`
func (p *HostAttributesProvider) Attributes() []attribute.KeyValue {
	result := []attribute.KeyValue{}

	for attrKey, hostAttr := range p.attributes {
		switch hostAttr {
		case "name":
			// Get hostname (matches Python: socket.gethostname())
			if hostname, err := os.Hostname(); err == nil {
				result = append(result, attribute.String(attrKey, hostname))
			}
		}
	}

	return result
}
