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

// `EnvironmentVariablesAttributesProvider` provides attributes from environment variables.
// Matches Python: `multistorageclient.telemetry.attributes.environment_variables.EnvironmentVariablesAttributesProvider`
type EnvironmentVariablesAttributesProvider struct {
	// Map of attribute key to environment variable name
	attributes map[string]string
}

// `NewEnvironmentVariablesAttributesProvider` creates a new environment variables attributes provider.
// Options should contain "attributes" key with a map[string]string mapping attribute keys to env var names.
// Matches Python: `EnvironmentVariablesAttributesProvider.__init__(attributes: Mapping[str, str])`
func NewEnvironmentVariablesAttributesProvider(options map[string]interface{}) *EnvironmentVariablesAttributesProvider {
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

	return &EnvironmentVariablesAttributesProvider{
		attributes: attrs,
	}
}

// `Attributes` returns attributes collected from environment variables.
// Only includes attributes where the environment variable is set.
// Matches Python: `EnvironmentVariablesAttributesProvider.attributes() -> api_types.Attributes`
func (p *EnvironmentVariablesAttributesProvider) Attributes() []attribute.KeyValue {
	result := []attribute.KeyValue{}

	for attrKey, envVarName := range p.attributes {
		// Only include if environment variable exists (matches Python behavior)
		if envValue, exists := os.LookupEnv(envVarName); exists {
			result = append(result, attribute.String(attrKey, envValue))
		}
	}

	return result
}
