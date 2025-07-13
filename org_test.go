package org

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/leisure-tools/lazyfingertree"
	u "github.com/leisure-tools/utils"
)

var blocks = []string{
	`
`, `#+TITLE: fred
`, `*  headline 1
`, `
`, `#+name: bubba

#+begin_src sh
echo hello
#+end_src
`, `
`, `#+NAME: data1
#+begin_src yaml
a: 1
b: 2
#+end_src
`, `
`, `#+NAME: data2
#+begin_src
a: 1
b: 2
#+end_src
`, `#+NAME: table1

| a | b | c |
|---+---+---|
| 1 | 2 | 3 |
| 4 |   | 5 |
`, `
`, `* headline 2
`, `#+begin_src bubba
`, `#+begin_src fred
a
#+end_src
`, `#+begin_src plurp
`, `* headline 3
`, `#+end_src
`,
}

var types = []OrgType{
	TextType,
	KeywordType,
	HeadlineType,
	TextType,
	SourceType,
	TextType,
	SourceType,
	TextType,
	SourceType,
	TableType,
	TextType,
	HeadlineType,
	TextType,
	SourceType,
	TextType,
	HeadlineType,
	TextType,
}

const LOC_DOC = `#+NAME: ctrl1
#+begin_src yaml :control :update 3 :root @person :owner julia :maluba "one two"
name:
number:
#+end_src
#+NAME: fred
#+begin_src yaml :tags one two
2
#+end_src
#+NAME: joe
#+begin_src yaml :tags two three
3
#+end_src`

var doc1 = strings.Join(blocks, "")

var insert1 = `#+begin_src json
{
	"a": 1,
	"b": 2
}
#+end_src
`

type myT struct {
	*testing.T
}

type fallible interface {
	Error(args ...any)
	FailNow()
}

func die(t fallible, args ...any) {
	fmt.Fprintln(os.Stderr, args...)
	t.Error(args...)
	debug.PrintStack()
	t.FailNow()
}

func (t myT) testEqual(actual any, expected any, format string, args ...any) {
	if actual != expected {
		msg := fmt.Sprintf(format, args...)
		die(t, fmt.Sprintf("%s, expected\n <%v> but got\n <%v>\n", msg, expected, actual))
	}
}

func (t myT) testDeepEqual(actual any, expected any, format string, args ...any) {
	if !reflect.DeepEqual(actual, expected) {
		msg := fmt.Sprintf(format, args...)
		die(t, fmt.Sprintf("%s, expected\n <%v> but got\n <%v>\n", msg, expected, actual))
	}
}

func (t myT) failIfError(err error, format string, args ...any) {
	if err != nil {
		die(t, fmt.Sprintf(format, args...))
	}
}

func (t myT) failNowIfNot(cond bool, format string, args ...any) {
	if !cond {
		die(t, fmt.Sprintf(format+"\n", args...))
	}
}

func (t myT) testType(ch Chunk, typ OrgType, format string, args ...any) {
	if ch.AsOrgChunk().Type != typ {
		msg := fmt.Sprintf(format, args...)
		die(t, fmt.Sprintf("%s expected type <%s> but got <%s>",
			msg, typeNames[typ], typeNames[ch.AsOrgChunk().Type]))
	}
}

func TestSimple(tt *testing.T) {
	t := myT{tt}
	chunkPile := Parse(doc1)
	chunks := chunkPile.Chunks.ToSlice()
	for i := 0; i < len(chunks) && i < len(blocks); i++ {
		t.testType(chunks[i], types[i], "block #%d", i)
		t.testEqual(chunks[i].AsOrgChunk().text(), blocks[i], "Block %d has wrong text", i)
	}
	t.testEqual(len(chunks), len(blocks), "Different number of blocks")
	hl1 := 2
	sr1 := 4
	sr2 := 6
	hChunk, ok := chunks[hl1].(*Headline)
	t.failNowIfNot(ok, "Chunk %d is not a headline", hl1)
	t.testEqual(hChunk.Level, 1, "Chunk %d level is not 1", hl1)
	t.testEqual(hChunk.Text[hChunk.Level+2:len(hChunk.Text)-1], "headline 1", "Chunk %d level is not 1", hl1)
	srcChunk, ok := chunks[sr1].(*SourceBlock)
	t.failNowIfNot(ok, "Chunk %d is not a source block", sr1)
	t.testEqual(srcChunk.LabelText(), "sh", "Chunk %d language is not sh", sr1)
	srcChunk, ok = chunks[sr2].(*SourceBlock)
	t.failNowIfNot(ok, "Chunk %d is not a source block", sr2)
	t.testEqual(srcChunk.LabelText(), "yaml", "Chunk %d language is not yaml", sr2)
	val, ok := srcChunk.Value.(map[string]any)
	t.failNowIfNot(ok, "Chunk %d value is not a string map", sr2)
	exp := `{"a":1,"b":2}`
	t.failNowIfNot(ok, fmt.Sprintf(`Chunk %d expected value <%s> but got <%s>`, sr2, exp, val))
	t.testEqual(len(val), 2, fmt.Sprintf(`Chunk %d expected value <%s> but got <%s>`, sr2, exp, val))
	t.testEqual(val["a"], 1, fmt.Sprintf(`Chunk %d expected value <%s> but got <%s>`, sr2, exp, val))
	t.testEqual(val["b"], 2, fmt.Sprintf(`Chunk %d expected value <%s> but got <%s>`, sr2, exp, val))
}

func TestReplacement(tt *testing.T) {
	t := myT{tt}
	blocks2 := make([]string, 0, len(blocks)+1)
	blocks2 = append(blocks2, blocks[:4]...)
	blocks2 = append(blocks2, insert1)
	blocks2 = append(blocks2, blocks[4:]...)
	text2 := strings.Join(blocks2, "")
	prefix := strings.Join(blocks[:4], "")
	suffix := strings.Join(blocks[4:], "")
	chunks2 := Parse(text2)
	t.testEqual(chunks2.getText(), prefix+insert1+suffix, "parsed text differs")
	chunks := Parse(doc1)
	changes := chunks.Replace(len(prefix), 0, insert1)
	t.testEqual(len(changes.Removed), 0, "removed blocks")
	t.testEqual(len(changes.Added), 1, "added blocks")
	t.testEqual(len(changes.Changed), 0, "changed blocks")
	ch := chunks.GetChunk(string(changes.Added.ToSlice()[0]))
	b, err := json.Marshal(ch)
	t.failIfError(err, "Error getting json for %v", ch)
	var obj any
	t.failIfError(json.Unmarshal(b, &obj), "Could not redecode json")
	m, ok := obj.(map[string]any)
	t.failNowIfNot(ok, "expected a string map")
	t.failNowIfNot(m["text"] == insert1, "Unexpected text")
}

func TestLocation(tt *testing.T) {
	t := myT{tt}
	t.testDeepEqual(u.NewSet("fred", "joe"), u.NewSet("fred").Union(u.NewSet("joe")), "Bad set union")
	chunkPile := Parse(LOC_DOC)
	chunks := chunkPile.Chunks.ToSlice()
	testName := func(meas OrgMeasure, name ...string) {
		names := u.NewSet(name...)
		t.testDeepEqual(names, meas.Names, "Error, %s does not match measurement %s", names, meas)
	}
	testChunkName := func(ch Chunk, name string) {
		names := u.NewSet(name)
		meas := orgMeasurer(true).Measure(ch)
		t.testDeepEqual(names, meas.Names, "Error, %s does not measure with name %s", lazyfingertree.Brief(ch), name)
	}
	testChunkName(chunks[0], "ctrl1")
	testChunkName(chunks[1], "fred")
	testChunkName(chunks[2], "joe")
	m := orgMeasurer(true)
	id := m.Identity()
	for i := range chunks {
		testName(m.Sum(id, m.Measure(chunks[i])), Name(chunks[i]))
	}
	testName(m.Sum(m.Sum(id, m.Measure(chunks[1])), m.Measure(chunks[2])), "fred", "joe")
	for _, name := range []string{"ctrl1", "fred", "joe"} {
		_, r := chunkPile.LocateChunkNamed(name)
		if r.IsEmpty() {
			verbosity = 1
			verbose(1, "\n\n\nERROR, COULD NOT FIND DATA BLOCK %s\n", name)
			DisplayChunks("", chunkPile.Chunks)
			verbose(1, "\nTREE\n")
			chunkPile.Chunks.Dump(os.Stderr, 2)
			verbose(1, "\n")
			verbosity = 0
		}
		t.failNowIfNot(!r.IsEmpty(), "Could not find node %s", name)
		t.testEqual(Name(r.Chunk), name, "node %#v is not named %s", r.Chunk, name)
		_, r = chunkPile.LocateChunk(r.AsOrgChunk().Id)
		if r.IsEmpty() {
			verbosity = 1
			verbose(1, "\n\n\nERROR, COULD NOT FIND BLOCK BY ID %s\n", r.AsOrgChunk().Id)
			DisplayChunks("", chunkPile.Chunks)
			verbose(1, "\nTREE\n")
			chunkPile.Chunks.Dump(os.Stderr, 2)
			verbose(1, "\n")
			verbosity = 0
		}
		t.failNowIfNot(!r.IsEmpty(), "Could not find node with id %s", r.AsOrgChunk().Id)
	}
	var empty u.Set[string]
	t.testDeepEqual(tags(chunks[0]), empty, "chunk 1 has wrong tags")
	t.testDeepEqual(tags(chunks[1]), u.NewSet("one", "two"), "chunk 1 has wrong tags")
	t.testDeepEqual(tags(chunks[2]), u.NewSet("two", "three"), "chunk 2 has wrong tags")
	ones := chunkPile.GetChunksTagged("one")
	t.testEqual(len(ones), 1, "Found bad tagged blocks")
	twos := chunkPile.GetChunksTagged("two")
	t.testEqual(len(twos), 2, "Found bad tagged blocks")
	threes := chunkPile.GetChunksTagged("three")
	t.testEqual(len(threes), 1, "Found bad tagged blocks")
}

func tags(ch Chunk) u.Set[string] {
	if blk, ok := ch.(Tagged); ok {
		return blk.Tags()
	}
	return nil
}
