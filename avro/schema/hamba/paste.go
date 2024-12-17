package paste

import (
	"bytes"
	"context"
	"io"
	"iter"
	"os"

	ha "github.com/hamba/avro/v2"

	ps "github.com/takanoriyanagitani/go-avro-paste-schema"
	. "github.com/takanoriyanagitani/go-avro-paste-schema/util"
)

const SchemaSizeMaxDefault int64 = 1048576

func FieldsToRecordSchemaHamba(
	fields []*ha.Field,
	name string,
	namespace string,
) (*ha.RecordSchema, error) {
	return ha.NewRecordSchema(
		name,
		namespace,
		fields,
	)
}

func PasteRecordSchemaHamba(
	schemas iter.Seq[*ha.RecordSchema],
) (*ha.RecordSchema, error) {
	var name string = ""
	var namespace string = ""

	fieldNameSet := map[string]struct{}{}

	var newFields []*ha.Field

	for s := range schemas {
		name = s.Name()
		namespace = s.Namespace()
		var fields []*ha.Field = s.Fields()
		for _, field := range fields {
			var fieldName string = field.Name()
			_, found := fieldNameSet[fieldName]
			if !found {
				fieldNameSet[fieldName] = struct{}{}
				newFields = append(newFields, field)
			}
		}
	}

	return FieldsToRecordSchemaHamba(newFields, name, namespace)
}

func PasteSchemaHamba(
	schemas iter.Seq[ha.Schema],
) (*ha.RecordSchema, error) {
	var i iter.Seq[*ha.RecordSchema] = func(yield func(*ha.RecordSchema) bool) {
		for s := range schemas {
			var r *ha.RecordSchema
			switch t := s.(type) {
			case *ha.RecordSchema:
				r = t
			default:
				continue
			}

			if !yield(r) {
				return
			}
		}
	}
	return PasteRecordSchemaHamba(i)
}

func PasteSchemaToRecord(
	schemas iter.Seq[ps.AvscBytes],
) (*ha.RecordSchema, error) {
	var i iter.Seq[ha.Schema] = func(yield func(ha.Schema) bool) {
		for raw := range schemas {
			parsed, e := ha.ParseBytes(raw)
			if nil == e {
				if !yield(parsed) {
					return
				}
			}
		}
	}
	return PasteSchemaHamba(i)
}

func PasteSchema(
	schemas iter.Seq[ps.AvscBytes],
) (ps.AvscBytes, error) {
	rec, e := PasteSchemaToRecord(schemas)
	if nil != e {
		return nil, e
	}
	return rec.MarshalJSON()
}

func FileLikeToBufferLimited(
	limit int64,
	fileLike io.ReadCloser,
	buf *bytes.Buffer,
) error {
	defer fileLike.Close()
	limited := &io.LimitedReader{
		R: fileLike,
		N: limit,
	}
	buf.Reset()
	_, e := io.Copy(buf, limited)
	return e
}

func FilenamesToPastedSchemaJson(
	schemaSizeMax int64,
	filenames iter.Seq[string],
) (ps.AvscBytes, error) {
	var schemaContents iter.Seq[ps.AvscBytes] = func(
		yield func(ps.AvscBytes) bool,
	) {
		var buf bytes.Buffer
		for filename := range filenames {
			file, e := os.Open(filename)
			if nil != e {
				return
			}

			e = FileLikeToBufferLimited(schemaSizeMax, file, &buf)
			if nil != e {
				return
			}

			if !yield(buf.Bytes()) {
				return
			}
		}
	}
	return PasteSchema(schemaContents)
}

func FilenamesToPastedSchemaJsonDefault(
	filenames iter.Seq[string],
) (ps.AvscBytes, error) {
	return FilenamesToPastedSchemaJson(SchemaSizeMaxDefault, filenames)
}

func SchemaFilenamesToPastedSchemaDefault(
	filenames iter.Seq[string],
) IO[ps.AvscBytes] {
	return func(_ context.Context) (ps.AvscBytes, error) {
		return FilenamesToPastedSchemaJsonDefault(filenames)
	}
}
