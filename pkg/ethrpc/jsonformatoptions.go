// Copyright © 2026 Kaleido, Inc.
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ethrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/url"
	"strings"

	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-evmconnect/internal/msgs"
	"github.com/hyperledger/firefly-signer/pkg/abi"
)

type JSONFormatOptions string

type JSONSerializerSet struct {
	Mode    abi.FormattingMode
	Integer abi.IntSerializer
	Float   abi.FloatSerializer
	Bytes   abi.ByteSerializer
	Address abi.AddressSerializer
	Pretty  bool
}

const DefaultJSONFormatOptions JSONFormatOptions = ""

const optObject = "object"
const optArray = "array"
const optString = "string"
const optHex0x = "hex-0x"
const optHex = "hex"
const optHexPlain = "hex-plain"
const optJSONNumber = "json-number"
const optBase64 = "base64"
const optChecksum = "checksum"
const optSelfDescribing = "self-describing"
const optPretty = "pretty"

var jsonFormatOptionSupportedValues = map[string][]string{
	"mode":    {optObject, optArray, optSelfDescribing},
	"number":  {optString, optHex0x, optHex, optJSONNumber},
	"bytes":   {optHex0x, optHex, optHexPlain, optBase64},
	"address": {optHex0x, optHex, optHexPlain, optChecksum},
	optPretty: {"true", "false"},
}

func unknownJSONFormatOptionNameError(ctx context.Context, option, value string) error {

	var jsonFormatOptionNames []string
	for name := range jsonFormatOptionSupportedValues {
		jsonFormatOptionNames = append(jsonFormatOptionNames, name)
	}
	parts := make([]string, len(jsonFormatOptionNames))
	for i, name := range jsonFormatOptionNames {
		parts[i] = fmt.Sprintf("%s (%s)", name, strings.Join(jsonFormatOptionSupportedValues[name], ", "))
	}
	return i18n.NewError(ctx, msgs.MsgUnknownJSONFormatOption, option, value, strings.Join(parts, "; "))
}

func unknownJSONFormatOptionValueError(ctx context.Context, option, value string) error {
	values := jsonFormatOptionSupportedValues[strings.ToLower(option)]
	return i18n.NewError(ctx, msgs.MsgUnknownJSONFormatOptionValue, value, option, strings.Join(values, ", "))
}

func (jfo JSONFormatOptions) GetABISerializer(ctx context.Context) (serializer *abi.Serializer, err error) {
	return jfo.getABISerializer(ctx, false)
}

func (jfo JSONFormatOptions) GetABISerializerIgnoreErrors(ctx context.Context) *abi.Serializer {
	serializer, _ := jfo.getABISerializer(ctx, true)
	return serializer
}

func (jfo JSONFormatOptions) getABISerializer(ctx context.Context, skipErrors bool) (serializer *abi.Serializer, err error) {
	ss, err := jfo.GetSerializerSet(ctx, skipErrors)
	if err == nil {
		serializer = ss.ABISerializer()
	}
	return serializer, err
}

func (jfo JSONFormatOptions) GetSerializerSet(ctx context.Context, skipErrors bool) (ss *JSONSerializerSet, err error) {
	ss = StandardSerializerSet()
	if len(jfo) == 0 {
		return ss, err
	}
	options, err := url.ParseQuery(string(jfo))
	if err != nil {
		if !skipErrors {
			return nil, i18n.WrapError(ctx, err, msgs.MsgInvalidJSONFormatOptions, jfo)
		}
	}
	for option, values := range options {
		for _, v := range values {
			switch strings.ToLower(option) {
			case "mode":
				switch strings.ToLower(v) {
				case optObject:
					ss.Mode = abi.FormatAsObjects
				case optArray:
					ss.Mode = abi.FormatAsFlatArrays
				case optSelfDescribing:
					ss.Mode = abi.FormatAsSelfDescribingArrays
				default:
					if !skipErrors {
						return nil, unknownJSONFormatOptionValueError(ctx, option, v)
					}
				}
			case "number":
				switch strings.ToLower(v) {
				case optString: // default
					ss.Integer = abi.Base10StringIntSerializer
				case optHex0x, optHex:
					ss.Integer = abi.HexIntSerializer0xPrefix
				case optJSONNumber: // note consumer must be very careful to use a JSON parser that support large numbers
					ss.Integer = abi.JSONNumberIntSerializer
				default:
					if !skipErrors {
						return nil, unknownJSONFormatOptionValueError(ctx, option, v)
					}
				}
			case "bytes":
				switch strings.ToLower(v) {
				case optHex0x, optHex:
					ss.Bytes = abi.HexByteSerializer0xPrefix
				case optHexPlain:
					ss.Bytes = abi.HexByteSerializer
				case optBase64:
					ss.Bytes = abi.Base64ByteSerializer
				default:
					if !skipErrors {
						return nil, unknownJSONFormatOptionValueError(ctx, option, v)
					}
				}
			case "address":
				switch strings.ToLower(v) {
				case optHex0x, optHex:
					ss.Address = abi.HexAddrSerializer0xPrefix
				case optHexPlain:
					ss.Address = abi.HexAddrSerializerPlain
				case optChecksum:
					ss.Address = abi.ChecksumAddrSerializer
				default:
					if !skipErrors {
						return nil, unknownJSONFormatOptionValueError(ctx, option, v)
					}
				}
			case optPretty:
				ss.Pretty = (v != "false")
			default:
				if !skipErrors {
					return nil, unknownJSONFormatOptionNameError(ctx, option, v)
				}
			}
		}
	}
	return ss, nil
}

// Marshalling options specific to the structure that is being marshalled,
// rather than JSONFormatOptions which are specific to the way to represent data in JSON
type MarshalOption struct {
	RedactFields   []string // helpful when you want to trim large fields
	OmitNullFields []string // a simplified approach to "omitempty" on JSON struct formatting tags
}

// MarshalFormattedMap takes a map that contains certain pre-selected types (see below switch) and does
// JSON marshalling adhering to the request of the users.
func (ss *JSONSerializerSet) MarshalFormattedMap(value map[string]any, opts ...MarshalOption) (data json.RawMessage, err error) {
	if ss.Pretty {
		return json.MarshalIndent(ss.buildFormatMap(value, opts), "", "  ")
	}
	return json.Marshal(ss.buildFormatMap(value, opts))
}

func (ss *JSONSerializerSet) buildFormatMap(valueMap map[string]any, opts []MarshalOption) map[string]any {
	formatMap := make(map[string]any)
	for k, v := range valueMap {
		redact := false
		for _, opt := range opts {
			for _, f := range opt.RedactFields {
				if k == f {
					redact = true
				}
			}
		}
		if !redact {
			fmtValue, isNil := ss.buildFormatElem(v, opts)
			redactNil := false
			if isNil {
				for _, opt := range opts {
					for _, f := range opt.OmitNullFields {
						if k == f {
							redactNil = true
						}
					}
				}
			}
			if !redactNil {
				formatMap[k] = fmtValue
			}
		}
	}
	return formatMap
}

func (ss *JSONSerializerSet) buildFormatElem(value any, opts []MarshalOption) (_ any, isNil bool) {
	switch vt := value.(type) {
	case *big.Int:
		isNil = (vt == nil)
		if !isNil {
			return ss.Integer(vt), false
		}
	case *uint64:
		isNil = (vt == nil)
		if !isNil {
			return ss.Integer(new(big.Int).SetUint64(*vt)), false
		}
	case *big.Float:
		isNil = (vt == nil)
		if !isNil {
			return ss.Float(vt), false
		}
	case []byte:
		isNil = (vt == nil)
		if !isNil {
			return ss.Bytes(vt), false
		}
	case *[20]byte:
		isNil = (vt == nil)
		if !isNil {
			return ss.Address(*vt), false
		}
	case []any:
		isNil = (vt == nil)
		if !isNil {
			ret := make([]any, len(vt))
			for i, av := range vt {
				ret[i], _ = ss.buildFormatElem(av, opts)
			}
			return ret, false
		}
	case map[string]any:
		isNil = (vt == nil)
		if !isNil {
			return ss.buildFormatMap(vt, opts), false
		}
	}
	return value, isNil
}

func StandardSerializerSet() *JSONSerializerSet {
	return &JSONSerializerSet{
		Mode:    abi.FormatAsObjects,
		Integer: abi.Base10StringIntSerializer,
		Float:   abi.Base10StringFloatSerializer,
		Bytes:   abi.HexByteSerializer0xPrefix,
		Address: abi.HexAddrSerializer0xPrefix,
	}
}

func (ss *JSONSerializerSet) ABISerializer() *abi.Serializer {
	return abi.NewSerializer().
		SetFormattingMode(ss.Mode).
		SetIntSerializer(ss.Integer).
		SetFloatSerializer(ss.Float).
		SetByteSerializer(ss.Bytes).
		SetAddressSerializer(ss.Address).
		SetPretty(ss.Pretty)
}

// The serializer we should use in all places that go from ABI validated data,
// back down to JSON that might be:
// 1) Passed to end-users over a JSON/RPC API
// 2) Passed to domain plugins over a gRPC API
func StandardABISerializer() *abi.Serializer {
	return StandardSerializerSet().ABISerializer()
}
