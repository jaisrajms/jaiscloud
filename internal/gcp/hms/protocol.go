package hms

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"
)

// maxValueDepth bounds struct/container nesting so a hostile or corrupt peer
// can't drive unbounded recursion in the codec.
const maxValueDepth = 64

var errDepth = errors.New("hms: thrift value nesting too deep")

// ReadStruct reads one Thrift struct from p (generic, lossless: fields in wire
// order, every value type-tagged).
func ReadStruct(ctx context.Context, p thrift.TProtocol) (*Struct, error) {
	if _, err := p.ReadStructBegin(ctx); err != nil {
		return nil, err
	}
	fields := make([]Field, 0, 4)
	for {
		_, typeID, id, err := p.ReadFieldBegin(ctx)
		if err != nil {
			return nil, err
		}
		if typeID == thrift.STOP {
			break
		}
		v, err := readValue(ctx, p, typeID, 0)
		if err != nil {
			return nil, err
		}
		if err := p.ReadFieldEnd(ctx); err != nil {
			return nil, err
		}
		fields = append(fields, Field{ID: id, V: v})
	}
	if err := p.ReadStructEnd(ctx); err != nil {
		return nil, err
	}
	return &Struct{Fields: fields}, nil
}

// WriteStruct writes s to p.
func WriteStruct(ctx context.Context, p thrift.TProtocol, s *Struct) error {
	if s == nil {
		s = &Struct{}
	}
	if err := p.WriteStructBegin(ctx, ""); err != nil {
		return err
	}
	for i := range s.Fields {
		f := s.Fields[i]
		if err := p.WriteFieldBegin(ctx, "", f.V.T, f.ID); err != nil {
			return err
		}
		if err := writeValue(ctx, p, f.V, 0); err != nil {
			return err
		}
		if err := p.WriteFieldEnd(ctx); err != nil {
			return err
		}
	}
	if err := p.WriteFieldStop(ctx); err != nil {
		return err
	}
	return p.WriteStructEnd(ctx)
}

func readValue(ctx context.Context, p thrift.TProtocol, t thrift.TType, depth int) (Value, error) {
	if depth > maxValueDepth {
		return Value{}, errDepth
	}
	switch t {
	case thrift.BOOL:
		v, err := p.ReadBool(ctx)
		return BoolV(v), err
	case thrift.BYTE:
		v, err := p.ReadByte(ctx)
		return ByteV(v), err
	case thrift.I16:
		v, err := p.ReadI16(ctx)
		return I16V(v), err
	case thrift.I32:
		v, err := p.ReadI32(ctx)
		return I32V(v), err
	case thrift.I64:
		v, err := p.ReadI64(ctx)
		return I64V(v), err
	case thrift.DOUBLE:
		v, err := p.ReadDouble(ctx)
		return DoubleV(v), err
	case thrift.STRING:
		// TBinaryProtocol encodes `binary` as STRING; reading as string and
		// re-writing as string is byte-identical, so binary round-trips too.
		v, err := p.ReadString(ctx)
		return StringV(v), err
	case thrift.STRUCT:
		s, err := ReadStruct(ctx, p)
		if err != nil {
			return Value{}, err
		}
		return StructV(s), nil
	case thrift.LIST:
		elem, size, err := p.ReadListBegin(ctx)
		if err != nil {
			return Value{}, err
		}
		elems, err := readElems(ctx, p, elem, size, depth+1)
		if err != nil {
			return Value{}, err
		}
		if err := p.ReadListEnd(ctx); err != nil {
			return Value{}, err
		}
		return ListV(elem, elems), nil
	case thrift.SET:
		elem, size, err := p.ReadSetBegin(ctx)
		if err != nil {
			return Value{}, err
		}
		elems, err := readElems(ctx, p, elem, size, depth+1)
		if err != nil {
			return Value{}, err
		}
		if err := p.ReadSetEnd(ctx); err != nil {
			return Value{}, err
		}
		return SetV(elem, elems), nil
	case thrift.MAP:
		kt, vt, size, err := p.ReadMapBegin(ctx)
		if err != nil {
			return Value{}, err
		}
		entries := make([]MapEntry, 0, size)
		for i := 0; i < size; i++ {
			k, err := readValue(ctx, p, kt, depth+1)
			if err != nil {
				return Value{}, err
			}
			v, err := readValue(ctx, p, vt, depth+1)
			if err != nil {
				return Value{}, err
			}
			entries = append(entries, MapEntry{K: k, V: v})
		}
		if err := p.ReadMapEnd(ctx); err != nil {
			return Value{}, err
		}
		return MapV(kt, vt, entries), nil
	}
	return Value{}, fmt.Errorf("hms: unsupported thrift type %d", t)
}

func readElems(ctx context.Context, p thrift.TProtocol, elem thrift.TType, size, depth int) ([]Value, error) {
	elems := make([]Value, 0, size)
	for i := 0; i < size; i++ {
		e, err := readValue(ctx, p, elem, depth)
		if err != nil {
			return nil, err
		}
		elems = append(elems, e)
	}
	return elems, nil
}

func writeValue(ctx context.Context, p thrift.TProtocol, v Value, depth int) error {
	if depth > maxValueDepth {
		return errDepth
	}
	switch v.T {
	case thrift.BOOL:
		return p.WriteBool(ctx, v.B)
	case thrift.BYTE:
		return p.WriteByte(ctx, v.I8)
	case thrift.I16:
		return p.WriteI16(ctx, v.I16)
	case thrift.I32:
		return p.WriteI32(ctx, v.I32)
	case thrift.I64:
		return p.WriteI64(ctx, v.I64)
	case thrift.DOUBLE:
		return p.WriteDouble(ctx, v.F64)
	case thrift.STRING:
		return p.WriteString(ctx, v.Str)
	case thrift.STRUCT:
		return WriteStruct(ctx, p, v.S)
	case thrift.LIST:
		if err := p.WriteListBegin(ctx, v.Elem, len(v.List)); err != nil {
			return err
		}
		for i := range v.List {
			if err := writeValue(ctx, p, v.List[i], depth+1); err != nil {
				return err
			}
		}
		return p.WriteListEnd(ctx)
	case thrift.SET:
		if err := p.WriteSetBegin(ctx, v.Elem, len(v.List)); err != nil {
			return err
		}
		for i := range v.List {
			if err := writeValue(ctx, p, v.List[i], depth+1); err != nil {
				return err
			}
		}
		return p.WriteSetEnd(ctx)
	case thrift.MAP:
		if err := p.WriteMapBegin(ctx, v.K, v.V, len(v.Entries)); err != nil {
			return err
		}
		for i := range v.Entries {
			if err := writeValue(ctx, p, v.Entries[i].K, depth+1); err != nil {
				return err
			}
			if err := writeValue(ctx, p, v.Entries[i].V, depth+1); err != nil {
				return err
			}
		}
		return p.WriteMapEnd(ctx)
	}
	return fmt.Errorf("hms: unsupported thrift type %d", v.T)
}
