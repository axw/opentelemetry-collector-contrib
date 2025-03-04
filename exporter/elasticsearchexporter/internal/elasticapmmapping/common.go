// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticapmmapping

import (
	"iter"
	"strconv"
	"strings"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/objmodel"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

const (
	keywordLength = 1024
	dot           = "."
	underscore    = "_"
)

// SetLabel sets labels.k=v or numeric_labels.k=v, with k dedotted,
// for value types supported by Elastic APM.
func SetLabel(doc *objmodel.Document, k string, v pcommon.Value) {
	k = replaceDots(k)

	// NOTE not all types are handled by Elastic APM;
	// we intenally handle no more and no less here.
	switch v.Type() {
	case pcommon.ValueTypeStr:
		doc.AddString("labels."+k, truncate(v.Str()))
	case pcommon.ValueTypeBool:
		doc.AddString("labels."+k, strconv.FormatBool(v.Bool()))
	case pcommon.ValueTypeInt, pcommon.ValueTypeDouble:
		doc.AddAttribute("numeric_labels."+k, v)
	case pcommon.ValueTypeSlice:
		s := v.Slice()
		n := s.Len()
		if n == 0 {
			return
		}
		newValue := pcommon.NewValueSlice()
		newSlice := newValue.Slice()
		newSlice.EnsureCapacity(n)
		switch typ := s.At(0).Type(); typ {
		case pcommon.ValueTypeStr:
			for elem := range matchingTypeValues(s, typ) {
				elem.CopyTo(newSlice.AppendEmpty())
			}
			doc.AddAttribute("labels."+k, newValue)
		case pcommon.ValueTypeBool:
			for elem := range matchingTypeValues(s, typ) {
				newSlice.AppendEmpty().SetStr(strconv.FormatBool(elem.Bool()))
			}
			doc.AddAttribute("labels."+k, newValue)
		case pcommon.ValueTypeDouble, pcommon.ValueTypeInt:
			for elem := range matchingTypeValues(s, typ) {
				elem.CopyTo(newSlice.AppendEmpty())
			}
			doc.AddAttribute("numeric_labels."+k, newValue)
		}
	}
}

func matchingTypeValues(s pcommon.Slice, typ pcommon.ValueType) iter.Seq[pcommon.Value] {
	return func(yield func(v pcommon.Value) bool) {
		n := s.Len()
		if n == 0 {
			return
		}
		s0 := s.At(0)
		typ := s0.Type()
		if !yield(s0) {
			return
		}
		for i := 1; i < n; i++ {
			si := s.At(i)
			if si.Type() == typ && !yield(si) {
				return
			}
		}
	}
}

func replaceDots(s string) string {
	return strings.ReplaceAll(s, dot, underscore)
}

// truncate returns s truncated at n runes, and the number of runes in the resulting string (<= n).
func truncate(s string) string {
	var j int
	for i := range s {
		if j == keywordLength {
			return s[:i]
		}
		j++
	}
	return s
}
