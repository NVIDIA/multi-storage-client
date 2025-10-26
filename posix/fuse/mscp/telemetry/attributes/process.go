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

// `ProcessAttributesProvider` provides attributes from current process information.
// Matches Python: `multistorageclient.telemetry.attributes.process.ProcessAttributesProvider`
type ProcessAttributesProvider struct {
	// Map of attribute key to process attribute type
	attributes map[string]string
}

// `NewProcessAttributesProvider` creates a new process attributes provider.
// Options should contain "attributes" key with a map[string]string mapping attribute keys to process attributes.
// Supported process attributes: "pid" (process ID)
// Matches Python: `ProcessAttributesProvider.__init__(attributes: Mapping[str, str])`
func NewProcessAttributesProvider(options map[string]interface{}) *ProcessAttributesProvider {
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

	return &ProcessAttributesProvider{
		attributes: attrs,
	}
}

// `Attributes` returns attributes collected from process information.
// Matches Python: `ProcessAttributesProvider.attributes() -> api_types.Attributes`
func (p *ProcessAttributesProvider) Attributes() []attribute.KeyValue {
	result := []attribute.KeyValue{}

	for attrKey, processAttr := range p.attributes {
		switch processAttr {
		case "pid":
			// Get process ID (matches Python: process.pid)
			result = append(result, attribute.Int(attrKey, os.Getpid()))
		}
	}

	return result
}
