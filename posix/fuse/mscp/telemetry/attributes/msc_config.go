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
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"strings"

	"github.com/jmespath/go-jmespath"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/crypto/sha3"
)

// `MSCConfigAttributesProvider` provides attributes from MSC configuration using JMESPath expressions.
// Matches Python: `multistorageclient.telemetry.attributes.msc_config.MSCConfigAttributesProvider`
//
// Note: Unlike Python's jmespath library which supports custom functions natively,
// Go's jmespath library does not. Therefore, we preprocess hash() calls before
// passing to JMESPath. This requires hash() to be at the start of the expression.
type MSCConfigAttributesProvider struct {
	// Precomputed attributes from config
	attributes []attribute.KeyValue
}

// `NewMSCConfigAttributesProvider` creates a new MSC config attributes provider.
// Options should contain:
//   - "attributes": map of attribute keys to attribute value options
//   - "config_dict": the full configuration dictionary to query
//
// Attribute value options should contain:
//   - "expression": JMESPath expression to extract value from config,
//   - Note: JMESPath is a query language for JSON/YAML data.
//
// Supports custom hash() function:
//   - hash('algorithm', jmespath_expression)
//   - Example: hash('sha3-224', backends[0].S3.secret_access_key)
//   - hash() must be at the start of the expression
//
// Matches Python: `MSCConfigAttributesProvider.__init__(attributes, config_dict)`
func NewMSCConfigAttributesProvider(options map[string]interface{}) *MSCConfigAttributesProvider {
	var configDict map[string]interface{}
	attrs := []attribute.KeyValue{}

	// Get the config dictionary
	if configDictInterface, ok := options["config_dict"]; ok {
		if cd, ok := configDictInterface.(map[string]interface{}); ok {
			configDict = cd
		}
	}

	// Get the attributes configuration
	if attrsInterface, ok := options["attributes"]; ok {
		if attrsMap, ok := attrsInterface.(map[string]interface{}); ok {
			for attrKey, valueOptions := range attrsMap {
				if voMap, ok := valueOptions.(map[string]interface{}); ok {
					// Extract the JMESPath expression
					if expression, ok := voMap["expression"].(string); ok {
						// Evaluate expression (supports hash() function)
						value, err := evalJMESPathWithHash(expression, configDict)
						if err != nil {
							// Log error but continue (matches Python behavior of returning nil)
							continue
						}
						if value != nil {
							// Convert value to attribute
							attrs = append(attrs, toAttribute(attrKey, value))
						}
					}
				}
			}
		}
	}

	return &MSCConfigAttributesProvider{
		attributes: attrs,
	}
}

// `Attributes` returns the precomputed attributes from configuration.
// Matches Python: `MSCConfigAttributesProvider.attributes() -> api_types.Attributes`
func (p *MSCConfigAttributesProvider) Attributes() []attribute.KeyValue {
	// Return a copy to avoid external modification
	result := make([]attribute.KeyValue, len(p.attributes))
	copy(result, p.attributes)
	return result
}

// evalJMESPathWithHash evaluates JMESPath expressions with custom hash() function support.
// Supports: hash('algorithm', jmespath_expression)
// Example: hash('sha3-224', backends[0].S3.secret_access_key)
//
// Note: Due to Go's jmespath library limitations, hash() must be at the start of the expression.
// This is simpler than full parsing but covers all practical use cases.
func evalJMESPathWithHash(expression string, data map[string]interface{}) (interface{}, error) {
	expression = strings.TrimSpace(expression)

	// Check if expression contains hash() function anywhere
	containsHash := strings.Contains(expression, "hash(")
	startsWithHash := strings.HasPrefix(expression, "hash(")

	// If hash() exists but not at the start, return a clear error
	if containsHash && !startsWithHash {
		return nil, fmt.Errorf("hash() function must be at the start of the expression, got: %s", expression)
	}

	// If no hash() function, treat as standard JMESPath
	if !containsHash {
		return jmespath.Search(expression, data)
	}

	// Parse hash('algorithm', jmespath_expr)
	if !strings.HasSuffix(expression, ")") {
		return nil, errors.New("hash() expression must end with ')'")
	}

	// Extract content between hash( and )
	args := strings.TrimPrefix(expression, "hash(")
	args = strings.TrimSuffix(args, ")")

	// Split on first comma only (allows commas in JMESPath expression)
	parts := strings.SplitN(args, ",", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("hash() requires 2 arguments: hash('algorithm', expression), got: %s", expression)
	}

	// Extract algorithm (remove quotes and whitespace)
	algorithm := strings.TrimSpace(parts[0])
	algorithm = strings.Trim(algorithm, "'\"")
	if algorithm == "" {
		return nil, errors.New("hash() algorithm cannot be empty")
	}

	// Extract and evaluate inner JMESPath expression
	valueExpr := strings.TrimSpace(parts[1])
	if valueExpr == "" {
		return nil, errors.New("hash() value expression cannot be empty")
	}

	// Evaluate the inner JMESPath expression
	value, err := jmespath.Search(valueExpr, data)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate hash() expression %q: %w", valueExpr, err)
	}
	if value == nil {
		return nil, nil
	}

	// Convert value to string for hashing
	strValue := fmt.Sprintf("%v", value)

	// Compute hash
	hashValue, err := computeHash(algorithm, strValue)
	if err != nil {
		return nil, fmt.Errorf("hash() failed: %w", err)
	}

	return hashValue, nil
}

// computeHash computes the hexadecimal hash digest of a string.
// Matches Python: hashlib.new(algorithm)
func computeHash(algorithm, value string) (string, error) {
	h, err := newHash(algorithm)
	if err != nil {
		return "", err
	}
	h.Write([]byte(value))
	return hex.EncodeToString(h.Sum(nil)), nil
}

// newHash returns a hash.Hash for the given algorithm name.
// Supports all algorithms from Python's hashlib that are commonly available.
func newHash(algo string) (hash.Hash, error) {
	switch algo {
	case "md5":
		return md5.New(), nil
	case "sha1":
		return sha1.New(), nil
	case "sha224":
		return sha256.New224(), nil
	case "sha256":
		return sha256.New(), nil
	case "sha384":
		return sha512.New384(), nil
	case "sha512":
		return sha512.New(), nil
	case "sha3-224":
		return sha3.New224(), nil
	case "sha3-256":
		return sha3.New256(), nil
	case "sha3-384":
		return sha3.New384(), nil
	case "sha3-512":
		return sha3.New512(), nil
	default:
		return nil, fmt.Errorf("unsupported hash algorithm: %s (supported: md5, sha1, sha224, sha256, sha384, sha512, sha3-224, sha3-256, sha3-384, sha3-512)", algo)
	}
}

// toAttribute converts a value to an OpenTelemetry attribute
func toAttribute(key string, value interface{}) attribute.KeyValue {
	switch v := value.(type) {
	case string:
		return attribute.String(key, v)
	case int:
		return attribute.Int(key, v)
	case int64:
		return attribute.Int64(key, v)
	case float64:
		return attribute.Float64(key, v)
	case bool:
		return attribute.Bool(key, v)
	default:
		// Convert to string for other types
		return attribute.String(key, fmt.Sprintf("%v", v))
	}
}
