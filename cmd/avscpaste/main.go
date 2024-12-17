package main

import (
	"bytes"
	"context"
	"io"
	"iter"
	"log"
	"os"
	"slices"

	ps "github.com/takanoriyanagitani/go-avro-paste-schema"
	. "github.com/takanoriyanagitani/go-avro-paste-schema/util"

	sh "github.com/takanoriyanagitani/go-avro-paste-schema/avro/schema/hamba"
)

var namesArray IO[[]string] = Of(os.Args[1:])

var filenames IO[iter.Seq[string]] = Bind(
	namesArray,
	Lift(func(names []string) (iter.Seq[string], error) {
		return slices.Values(names), nil
	}),
)

var names2pasted func(iter.Seq[string]) IO[ps.AvscBytes] = sh.
	SchemaFilenamesToPastedSchemaDefault

var pastedSchema IO[ps.AvscBytes] = Bind(
	filenames,
	names2pasted,
)

func pastedSchemaSink(pasted ps.AvscBytes) IO[Void] {
	return func(_ context.Context) (Void, error) {
		var rdr io.Reader = bytes.NewReader(pasted)
		_, e := io.Copy(os.Stdout, rdr)
		return Empty, e
	}
}

var schemaFilenamesToPastedSchemaToSink IO[Void] = Bind(
	pastedSchema,
	pastedSchemaSink,
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	return schemaFilenamesToPastedSchemaToSink(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
