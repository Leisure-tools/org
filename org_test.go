package org

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"testing"
)

var blocks = []string{
	`#+TITLE: fred
`,
	`*  headline 1
`,
	`
`,
	`#+name: bubba

#+begin_src sh
echo hello
#+end_src
`,
	`
`,
	`#+begin_src yaml
a: 1
b: 2
#+end_src
`,
	`
`,
	`#+begin_src
a: 1
b: 2
#+end_src
`,
}

var types = []OrgType{
	KeywordType,
	HeadlineType,
	TextType,
	SourceType,
	TextType,
	SourceType,
	TextType,
	SourceType,
}

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

func (t myT) failIfError(err error, format string, args ...any) {
	if err != nil {
		die(t, fmt.Sprintf(format, args...))
	}
}

func (t myT) failNowIfNot(cond bool, msg any) {
	if !cond {
		die(t, fmt.Sprint(msg, "\n"))
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
	hChunk, ok := chunks[1].(*Headline)
	t.failNowIfNot(ok, "Chunk 2 is not a headline")
	t.testEqual(hChunk.Level, 1, "Chunk 2 level is not 1")
	t.testEqual(hChunk.Text[hChunk.Level+2:len(hChunk.Text)-1], "headline 1", "Chunk 2 level is not 1")
	srcChunk, ok := chunks[3].(*SourceBlock)
	t.failNowIfNot(ok, "Chunk 4 is not a source block")
	t.testEqual(srcChunk.LabelText(), "sh", "Chunk 3 language is not sh")
	srcChunk, ok = chunks[5].(*SourceBlock)
	t.failNowIfNot(ok, "Chunk 6 is not a source block")
	t.testEqual(srcChunk.LabelText(), "yaml", "Chunk 6 language is not yaml")
	val, ok := srcChunk.Value.(map[string]any)
	t.failNowIfNot(ok, "Chunk 6 value is not a string map")
	exp := `{"a":1,"b":2}`
	t.failNowIfNot(ok, fmt.Sprintf(`Chunk 6 expected value <%s> but got <%s>`, exp, val))
	t.testEqual(len(val), 2, fmt.Sprintf(`Chunk 6 expected value <%s> but got <%s>`, exp, val))
	t.testEqual(val["a"], 1, fmt.Sprintf(`Chunk 6 expected value <%s> but got <%s>`, exp, val))
	t.testEqual(val["b"], 2, fmt.Sprintf(`Chunk 6 expected value <%s> but got <%s>`, exp, val))
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
