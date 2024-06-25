// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package objmodel

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/elastic/go-structform/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

var dijkstra = time.Date(1930, 5, 11, 16, 33, 11, 123456789, time.UTC)

func TestObjectModel_CreateMap(t *testing.T) {
	tests := map[string]struct {
		build func() Document
		want  Document
	}{
		"from map": {
			build: func() (doc Document) {
				m := pcommon.NewMap()
				m.PutInt("i", 42)
				m.PutStr("str", "test")
				doc.AddAttributes("", m)
				return doc
			},
			want: Document{[]field{{"i", intValue(42)}, {"str", stringValue("test")}}},
		},
		"ignores nil values": {
			build: func() (doc Document) {
				m := pcommon.NewMap()
				m.PutEmpty("null")
				m.PutStr("str", "test")
				doc.AddAttributes("", m)
				return doc
			},
			want: Document{[]field{{"str", stringValue("test")}}},
		},
		"add attributes with key": {
			build: func() (doc Document) {
				m := pcommon.NewMap()
				m.PutInt("i", 42)
				m.PutStr("str", "test")
				doc.AddAttributes("prefix", m)
				return doc
			},
			want: Document{[]field{{"prefix.i", intValue(42)}, {"prefix.str", stringValue("test")}}},
		},
		"add attribute flattens a map value": {
			build: func() (doc Document) {
				mapVal := pcommon.NewValueMap()
				m := mapVal.Map()
				m.PutInt("i", 42)
				m.PutStr("str", "test")
				doc.AddAttribute("prefix", mapVal)
				return doc
			},
			want: Document{[]field{{"prefix.i", intValue(42)}, {"prefix.str", stringValue("test")}}},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			doc := test.build()
			doc.Sort()
			assert.Equal(t, test.want, doc)
		})
	}
}

func TestDocument_Sort(t *testing.T) {
	tests := map[string]struct {
		build func() Document
		want  Document
	}{
		"keys are sorted": {
			build: func() (doc Document) {
				doc.AddInt("z", 26)
				doc.AddInt("a", 1)
				return doc
			},
			want: Document{[]field{{"a", intValue(1)}, {"z", intValue(26)}}},
		},
		"sorting is stable": {
			build: func() (doc Document) {
				doc.AddInt("a", 1)
				doc.AddInt("c", 3)
				doc.AddInt("a", 2)
				return doc
			},
			want: Document{[]field{{"a", intValue(1)}, {"a", intValue(2)}, {"c", intValue(3)}}},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			doc := test.build()
			doc.Sort()
			assert.Equal(t, test.want, doc)
		})
	}

}

func TestObjectModel_Dedup(t *testing.T) {
	tests := map[string]struct {
		build func() Document
		want  Document
	}{
		"no duplicates": {
			build: func() (doc Document) {
				doc.AddInt("a", 1)
				doc.AddInt("c", 3)
				return doc
			},
			want: Document{[]field{{"a", intValue(1)}, {"c", intValue(3)}}},
		},
		"duplicate keys": {
			build: func() (doc Document) {
				doc.AddInt("a", 1)
				doc.AddInt("c", 3)
				doc.AddInt("a", 2)
				return doc
			},
			want: Document{[]field{{"a", ignoreValue}, {"a", intValue(2)}, {"c", intValue(3)}}},
		},
		"duplicate after flattening from map: namespace object at end": {
			build: func() (doc Document) {
				am := pcommon.NewMap()
				am.PutInt("namespace.a", 42)
				am.PutStr("toplevel", "test")
				am.PutEmptyMap("namespace").PutInt("a", 23)
				doc.AddAttributes("", am)
				return doc
			},
			want: Document{[]field{{"namespace.a", ignoreValue}, {"namespace.a", intValue(23)}, {"toplevel", stringValue("test")}}},
		},
		"duplicate after flattening from map: namespace object at beginning": {
			build: func() (doc Document) {
				am := pcommon.NewMap()
				am.PutEmptyMap("namespace").PutInt("a", 23)
				am.PutInt("namespace.a", 42)
				am.PutStr("toplevel", "test")
				doc.AddAttributes("", am)
				return doc
			},
			want: Document{[]field{{"namespace.a", ignoreValue}, {"namespace.a", intValue(42)}, {"toplevel", stringValue("test")}}},
		},
		/*
			"dedup in arrays": {
				build: func() (doc Document) {
					m := pcommon.NewMap()
					m.PutInt("a", 1)
					m.PutInt("c", 3)
					m.PutInt("a", 2)
					doc.Add("arr", ObjectValue(m))
					return doc
				},
				want: Document{[]field{{"arr", arrValue(Value{kind: KindObject, obj: []field{
					{"a", ignoreValue},
					{"a", intValue(2)},
					{"c", intValue(3)},
				}})}}},
			},
		*/
		"dedup mix of primitive and object lifts primitive": {
			build: func() (doc Document) {
				doc.AddInt("namespace", 1)
				doc.AddInt("namespace.a", 2)
				return doc
			},
			want: Document{[]field{{"namespace.a", intValue(2)}, {"namespace.value", intValue(1)}}},
		},
		"dedup removes primitive if value exists": {
			build: func() (doc Document) {
				doc.AddInt("namespace", 1)
				doc.AddInt("namespace.a", 2)
				doc.AddInt("namespace.value", 3)
				return doc
			},
			want: Document{[]field{{"namespace.a", intValue(2)}, {"namespace.value", ignoreValue}, {"namespace.value", intValue(3)}}},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			doc := test.build()
			doc.Sort()
			doc.Dedup()
			assert.Equal(t, test.want, doc)
		})
	}
}

func TestValue_FromAttribute(t *testing.T) {
	tests := map[string]struct {
		in   pcommon.Value
		want Value
	}{
		"null": {
			in:   pcommon.NewValueEmpty(),
			want: nilValue,
		},
		"string": {
			in:   pcommon.NewValueStr("test"),
			want: stringValue("test"),
		},
		"int": {
			in:   pcommon.NewValueInt(23),
			want: intValue(23),
		},
		"double": {
			in:   pcommon.NewValueDouble(3.14),
			want: doubleValue(3.14),
		},
		"bool": {
			in:   pcommon.NewValueBool(true),
			want: boolValue(true),
		},
		"empty array": {
			in:   pcommon.NewValueSlice(),
			want: Value{kind: KindArr},
		},
		"non-empty array": {
			in: func() pcommon.Value {
				v := pcommon.NewValueSlice()
				tgt := v.Slice().AppendEmpty()
				pcommon.NewValueInt(1).CopyTo(tgt)
				return v
			}(),
			want: arrValue(intValue(1)),
		},
		"empty map": {
			in:   pcommon.NewValueMap(),
			want: Value{kind: KindObject, obj: nil},
		},
		"non-empty map": {
			in: func() pcommon.Value {
				v := pcommon.NewValueMap()
				v.Map().PutInt("a", 1)
				return v
			}(),
			want: Value{kind: KindObject, obj: []field{{"a", intValue(1)}}},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			v := valueFromAttribute(test.in)
			assert.Equal(t, test.want, v)
		})
	}
}

func TestDocument_Serialize_Flat(t *testing.T) {
	tests := map[string]struct {
		attrs map[string]any
		want  string
	}{
		"no nesting with multiple fields": {
			attrs: map[string]any{
				"a": "test",
				"b": 1,
			},
			want: `{"a":"test","b":1}`,
		},
		"shared prefix": {
			attrs: map[string]any{
				"a.str": "test",
				"a.i":   1,
			},
			want: `{"a.i":1,"a.str":"test"}`,
		},
		"multiple namespaces with dot": {
			attrs: map[string]any{
				"a.str": "test",
				"b.i":   1,
			},
			want: `{"a.str":"test","b.i":1}`,
		},
		"nested maps": {
			attrs: map[string]any{
				"a": map[string]any{
					"str": "test",
					"i":   1,
				},
			},
			want: `{"a.i":1,"a.str":"test"}`,
		},
		"multi-level nested namespace maps": {
			attrs: map[string]any{
				"a": map[string]any{
					"b.str": "test",
					"i":     1,
				},
			},
			want: `{"a.b.str":"test","a.i":1}`,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var doc Document
			var buf strings.Builder
			m := pcommon.NewMap()
			assert.NoError(t, m.FromRaw(test.attrs))
			doc.AddAttributes("", m)
			doc.Dedup()
			err := doc.Serialize(&buf, false)
			require.NoError(t, err)

			assert.Equal(t, test.want, buf.String())
		})
	}
}

func TestDocument_Serialize_Dedot(t *testing.T) {
	tests := map[string]struct {
		attrs map[string]any
		want  string
	}{
		"no nesting with multiple fields": {
			attrs: map[string]any{
				"a": "test",
				"b": 1,
			},
			want: `{"a":"test","b":1}`,
		},
		"shared prefix": {
			attrs: map[string]any{
				"a.str": "test",
				"a.i":   1,
			},
			want: `{"a":{"i":1,"str":"test"}}`,
		},
		"multiple namespaces": {
			attrs: map[string]any{
				"a.str": "test",
				"b.i":   1,
			},
			want: `{"a":{"str":"test"},"b":{"i":1}}`,
		},
		"nested maps": {
			attrs: map[string]any{
				"a": map[string]any{
					"str": "test",
					"i":   1,
				},
			},
			want: `{"a":{"i":1,"str":"test"}}`,
		},
		"multi-level nested namespace maps": {
			attrs: map[string]any{
				"a": map[string]any{
					"b.c.str": "test",
					"i":       1,
				},
			},
			want: `{"a":{"b":{"c":{"str":"test"}},"i":1}}`,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var doc Document
			var buf strings.Builder
			m := pcommon.NewMap()
			assert.NoError(t, m.FromRaw(test.attrs))
			doc.AddAttributes("", m)
			doc.Dedup()
			err := doc.Serialize(&buf, true)
			require.NoError(t, err)

			assert.Equal(t, test.want, buf.String())
		})
	}
}

func TestValue_Serialize(t *testing.T) {
	tests := map[string]struct {
		value Value
		want  string
	}{
		"nil value":         {value: nilValue, want: "null"},
		"bool value: true":  {value: boolValue(true), want: "true"},
		"bool value: false": {value: boolValue(false), want: "false"},
		"int value":         {value: intValue(42), want: "42"},
		"double value":      {value: doubleValue(3.14), want: "3.14"},
		"NaN is undefined":  {value: doubleValue(math.NaN()), want: "null"},
		"Inf is undefined":  {value: doubleValue(math.Inf(0)), want: "null"},
		"string value":      {value: stringValue("Hello World!"), want: `"Hello World!"`},
		"timestamp": {
			value: timestampValue(dijkstra),
			want:  `"1930-05-11T16:33:11.123456789Z"`,
		},
		"array": {
			value: arrValue(boolValue(true), intValue(23)),
			want:  `[true,23]`,
		},
		"object": {
			value: func() Value {
				return Value{kind: KindObject, obj: []field{{key: "a", value: stringValue("b")}}}
			}(),
			want: `{"a":"b"}`,
		},
		"empty object": {
			value: Value{kind: KindObject, obj: nil},
			want:  "null",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var buf strings.Builder
			err := test.value.iterJSON(json.NewVisitor(&buf), false)
			require.NoError(t, err)
			assert.Equal(t, test.want, buf.String())
		})
	}
}
