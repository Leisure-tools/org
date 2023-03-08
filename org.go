package org

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode"

	// support for low-res parsing of org-mode files
	// this parses into blocks and tracks character offsets

	"github.com/BurntSushi/toml"
	doc "github.com/leisure-tools/document"
	ft "github.com/leisure-tools/lazyfingertree"
	diff "github.com/sergi/go-diff/diffmatchpatch"
	"gopkg.in/yaml.v3"
)

type OrgType int64

type maluba = diff.Diff

var verbosity = 0

const (
	HeadlineType OrgType = iota
	TextType
	SourceType
	BlockType
	ResultsType
	HtmlType
	DrawerType
	KeywordType
	TableType
)

var typeNames = map[OrgType]string{
	HeadlineType: "headline",
	TextType:     "text",
	SourceType:   "source",
	BlockType:    "block",
	ResultsType:  "results",
	HtmlType:     "html",
	DrawerType:   "drawer",
	KeywordType:  "keyword",
	TableType:    "table",
}

var regexs map[string]*regexp.Regexp

func SetVerbosity(level int) {
	verbosity = level
}

func verbose(level int, format string, args ...any) {
	if level >= verbosity {
		fmt.Printf(format, args...)
	}
}

func re(name, value string) *regexp.Regexp {
	exp, err := regexp.Compile(value)
	if err != nil {
		panic(fmt.Sprintf("Error compiling regexp %s: %s", name, value))
	}
	return exp
}

type OrgStorage interface {
	GetBlock(name string) string
	StoreBlock(name string)
	RemoveBlock(name string)
}

var headlineRE = re(`headline`, `(?m)^(\*+) +(\S.*)?$`)
var keywordRE = re(`keyword`, `(?m)^#\+([^: \n]+): *(.*)$`)
var drawerRE = re(`drawer`, `(?m)^:([^: \n]+): *$`)
var drawerEndRE = re(`drawer-end`, `(?im)^:end: *$`)
var blockStartRE = re(`block-start`, `(?im)^(?:#\+begin_(\S+)|#\+begin:)(.*)$`)
var blockEndRE = re(`block-end`, `(?im)^(?:#\+end_(\S+)|#\+end:)(.*)$`)
var tableRowRE = re(`table`, `(?m)^ *\|.*\| *$`)
var htmlStartRE = re(`html-start`, `(?im)^#\+begin_html *(\S+)?(?: +(.*))?$`)
var srcStartRE = re(`src-start`, `(?im)^#\+begin_src *(\S+)?(?: +(.*))?$`)
var srcEndRE = re(`src-end`, `(?im)^#\+end_src *$`)
var resultsRE = re(`results`, `(?im)^(#+results:(.*)|results:) *$`)

/// orgTree

type orgTree = ft.FingerTree[orgMeasurer, Chunk, OrgMeasure]

type orgMeasurer bool

type OrgMeasure struct {
	Count int
	Width int
	Names doc.Set[string]
	Ids   doc.Set[OrgId]
}

func measure(ch Chunk) OrgMeasure {
	return orgMeasurer(true).Measure(ch)
}

func (m orgMeasurer) Identity() OrgMeasure {
	return OrgMeasure{}
}

func (m orgMeasurer) Measure(blk Chunk) OrgMeasure {
	names := doc.Set[string](nil)
	if src, ok := blk.(*SourceBlock); ok {
		names = doc.NewSet(src.Name())
	} else if tbl, ok := blk.(*TableBlock); ok {
		names = doc.NewSet(tbl.Name())
	}
	return OrgMeasure{
		Count: 1,
		Width: len(blk.AsOrgChunk().Text),
		Names: names,
		Ids:   doc.NewSet(blk.AsOrgChunk().Id),
	}
}

func (m orgMeasurer) Sum(a OrgMeasure, b OrgMeasure) OrgMeasure {
	return OrgMeasure{
		Count: a.Count + b.Count,
		Width: a.Width + b.Width,
		Names: a.Names.Union(b.Names),
		Ids:   a.Ids.Union(b.Ids),
	}
}

/// OrgChunks

type OrgId string

type idSet = doc.Set[OrgId]

type Jsonable interface {
	jsonRep(chunks *OrgChunks) map[string]any
}

type OrgChunks struct {
	ChunkIds    map[OrgId]Chunk
	Chunks      orgTree
	PendingText *strings.Builder
	MaxId       int
	Next        map[OrgId]OrgId
	Prev        map[OrgId]OrgId
	Parent      map[OrgId]OrgId
	Children    map[OrgId][]OrgId
}

// OrgBlocks are self-contained and offer an alternative to text updates
// Concatenating the text of all blocks produces the complete document
type BasicChunk struct {
	Type OrgType
	Id   OrgId
	Text string
}

type Headline struct {
	BasicChunk
	Level int
}

type Keyword struct {
	BasicChunk
	Label    int
	LabelEnd int
	Content  int
}

type Block struct {
	BasicChunk
	Label    int
	LabelEnd int
	Content  int
	End      int // start of last line
}

type DataBlock interface {
	Chunk
	SetValue(value any) (str string, err error)
}

// if this has children, the last one will be a results block
type SourceBlock struct {
	Block
	Options []string
	Value   any // parsed value for supported data types
	// these are relevant only if there is a preceding name element
	NameStart int
	NameEnd   int // this is 0 if there is no name
	SrcStart  int // this is 0 if there is no name
}

type TableBlock struct {
	BasicChunk
	Cells [][]string // 2D array of cell strings
	Value any        // 2D array of JSON-compatible values
	// these are relevant only if there is a preceding name element
	NameStart int
	NameEnd   int // this is 0 if there is no name
	TblStart  int // this is 0 if there is no name
}

type orgParser struct {
	*OrgChunks
	pendingText *strings.Builder
}

type ComparableChunk interface {
	comparable
	Chunk
}

type Chunk interface {
	Jsonable
	AsOrgChunk() *BasicChunk
	text() string
}

type ChunkRef struct {
	Chunk
	*OrgChunks
}

var illegalBlockContent = doc.NewSet(KeywordType, BlockType, SourceType)

func addIntProp(name string, value int, m map[string]any) {
	if value >= 0 {
		m[name] = value
	}
}

func addIdProp(name string, value OrgId, m map[string]any) {
	if value != "" {
		m[name] = value
	}
}

func (ch *BasicChunk) jsonRep(chunks *OrgChunks) map[string]any {
	id := ch.Id
	m := map[string]any{
		"type": typeNames[ch.Type],
		"id":   id,
		"text": ch.Text,
	}
	addIdProp("prev", chunks.Prev[id], m)
	addIdProp("next", chunks.Next[id], m)
	addIdProp("parent", chunks.Parent[id], m)
	if len(chunks.Children[id]) > 0 {
		m["children"] = chunks.Children[id]
	}
	return m
}

func (ch *BasicChunk) AsOrgChunk() *BasicChunk {
	return ch
}

func (ch *Headline) jsonRep(chunks *OrgChunks) map[string]any {
	rep := ch.BasicChunk.jsonRep(chunks)
	rep["level"] = ch.Level
	return rep
}

func (ch *Block) LabelText() string {
	if ch.Label == -1 {
		return ""
	}
	return ch.Text[ch.Label:ch.LabelEnd]
}

func (ch *Block) jsonRep(chunks *OrgChunks) map[string]any {
	rep := ch.BasicChunk.jsonRep(chunks)
	addIntProp("label", ch.Label, rep)
	addIntProp("labelEnd", ch.LabelEnd, rep)
	rep["content"] = ch.Content
	rep["end"] = ch.End
	return rep
}

func (ch *SourceBlock) Language() string {
	return strings.ToLower(ch.LabelText())
}

func (ch *SourceBlock) Name() string {
	if ch.NameStart == -1 || ch.NameEnd <= ch.NameStart {
		return ""
	}
	return ch.Text[ch.NameStart:ch.NameEnd]
}

func (ch *SourceBlock) IsNamedData() bool {
	return ch.IsData() && ch.Name() != ""
}

func (ch *SourceBlock) IsData() bool {
	lang := ch.Language()
	return lang == "json" || lang == "yaml" || lang == "toml"
}

func (ch *SourceBlock) SetValue(value any) (str string, err error) {
	sb := strings.Builder{}
	sb.WriteString(ch.Text[:ch.Content])
	switch ch.Language() {
	case "json":
		err = json.NewEncoder(&sb).Encode(value)
	case "yaml":
		err = yaml.NewEncoder(&sb).Encode(value)
	case "toml":
		err = toml.NewEncoder(&sb).Encode(value)
	}
	if err == nil {
		str = sb.String()
		if str[len(str)-1] != '\n' {
			sb.WriteRune('\n')
		}
		sb.WriteString(ch.Text[ch.End:])
		str = sb.String()
	}
	return
}

func (ch *SourceBlock) jsonRep(chunks *OrgChunks) map[string]any {
	rep := ch.Block.jsonRep(chunks)
	if ch.Options != nil {
		rep["options"] = ch.Options
	}
	if ch.IsData() {
		rep["value"] = ch.Value
	}
	if ch.NameStart != 0 {
		rep["nameStart"] = ch.NameStart
	}
	if ch.NameEnd != 0 {
		rep["nameEnd"] = ch.NameEnd
	}
	if ch.SrcStart != 0 {
		rep["srcStart"] = ch.SrcStart
	}
	return rep
}

func (ch *TableBlock) Name() string {
	if ch.NameStart == -1 || ch.NameEnd <= ch.NameStart {
		return ""
	}
	return ch.Text[ch.NameStart:ch.NameEnd]
}

func (ch *TableBlock) SetValue(value any) (str string, err error) {
	if rows, ok := value.([][]string); !ok || len(rows) == 0 {
		err = fmt.Errorf("value must be a rectangular [][]string with at least one row")
	} else {
		sb := strings.Builder{}
		sb.WriteString(ch.Text[:ch.TblStart])
		size := len(rows[0])
		for _, row := range rows {
			if len(row) != size {
				err = fmt.Errorf("value must be a rectangular [][]string with at least one row")
				return
			}
			sb.WriteString("|")
			for _, cell := range row {
				sb.WriteString(" ")
				sb.WriteString(cell)
				sb.WriteString(" |")
			}
			sb.WriteRune('\n')
		}
		str = sb.String()
	}
	return
}

func (ref ChunkRef) MarshalJSON() ([]byte, error) {
	if ref.Chunk == nil {
		return []byte("null"), nil
	}
	return json.Marshal(ref.Chunk.jsonRep(ref.OrgChunks))
}

func (blk ChunkRef) ref(id OrgId) ChunkRef {
	return ChunkRef{blk.ChunkIds[id], blk.OrgChunks}
}

func (blk ChunkRef) isEmpty() bool {
	return blk.Chunk == nil
}

func (blk *BasicChunk) text() string { return blk.AsOrgChunk().Text }

func (blk ChunkRef) Children() []OrgId {
	return blk.OrgChunks.Children[blk.AsOrgChunk().Id]
}

func (blk ChunkRef) Parent() OrgId {
	return blk.OrgChunks.Parent[blk.AsOrgChunk().Id]
}

func (blk ChunkRef) Prev() OrgId {
	return blk.OrgChunks.Prev[blk.AsOrgChunk().Id]
}

func (blk ChunkRef) Next() OrgId {
	return blk.OrgChunks.Next[blk.AsOrgChunk().Id]
}

func (blk ChunkRef) AllText(sb strings.Builder) {
	sb.WriteString(blk.text())
	for _, child := range blk.Children() {
		blk.ref(child).AllText(sb)
	}
}

func NewOrgChunks() *OrgChunks {
	return &OrgChunks{
		ChunkIds:    make(map[OrgId]Chunk, 32),
		Chunks:      ft.FromArray[orgMeasurer, Chunk, OrgMeasure](orgMeasurer(true), nil),
		PendingText: &strings.Builder{},
		Next:        map[OrgId]OrgId{},
		Prev:        map[OrgId]OrgId{},
		Parent:      map[OrgId]OrgId{},
		Children:    map[OrgId][]OrgId{},
	}
}

func Parse(doc string) *OrgChunks {
	blks := NewOrgChunks()
	for doc != "" {
		newDoc := blks.parseChunk(eatLine(doc))
		if len(doc) == len(newDoc) {
			panic(fmt.Sprintf("Did not parse document at %s", doc))
		}
		doc = newDoc
	}
	blks.addPending()
	prev := Chunk(nil)
	blks.Chunks.Each(func(ch Chunk) bool {
		if prev != nil {
			blks.Next[prev.AsOrgChunk().Id] = ch.AsOrgChunk().Id
			blks.Prev[ch.AsOrgChunk().Id] = prev.AsOrgChunk().Id
		}
		prev = ch
		return true
	})
	blks.RelinkHierarchy(nil)
	return blks
}

func eatLine(doc string) (string, string) {
	line := doc
	rest := ""
	if nl := strings.IndexRune(doc, '\n'); nl > -1 {
		line = doc[:nl+1]
		rest = doc[nl+1:]
	}
	return line, rest
}

func (chunks *OrgChunks) indexOf(ch Chunk) int {
	left, right := chunks.Chunks.Split(func(m OrgMeasure) bool {
		return m.Ids.Has(ch.AsOrgChunk().Id)
	})
	if right.IsEmpty() {
		return -1
	}
	return left.Measure().Count
}

func (chunks *OrgChunks) Sort(chunkList []Chunk) {
	positions := make(map[Chunk]int, len(chunkList))
	for _, chunk := range chunkList {
		positions[chunk] = chunks.indexOf(chunk)
	}
	sort.Slice(chunkList, func(i, j int) bool {
		return positions[chunkList[i]] < positions[chunkList[j]]
	})
}

func (chunks *OrgChunks) MarshalJSON() ([]byte, error) {
	out := make([]Chunk, 0, chunks.Chunks.Measure().Count)
	chunks.Chunks.Each(func(ch Chunk) bool {
		out = append(out, ChunkRef{ch, chunks})
		return true
	})
	return json.Marshal(out)
}

func (chunks *OrgChunks) LocateChunkNamed(name string) (int, ChunkRef) {
	left, right := chunks.Chunks.Split(func(m OrgMeasure) bool {
		return m.Names.Has(name)
	})
	if !right.IsEmpty() {
		return left.Measure().Width, ChunkRef{right.PeekFirst(), chunks}
	}
	return 0, ChunkRef{}
}

func (chunks *OrgChunks) GetChunkNamed(name string) ChunkRef {
	_, result := chunks.LocateChunkNamed(name)
	return result
}

func (chunks *OrgChunks) GetChunk(id string) ChunkRef {
	return ChunkRef{
		Chunk:     chunks.ChunkIds[OrgId(id)],
		OrgChunks: chunks,
	}
}

func (chunks *OrgChunks) getText() string {
	return getText(chunks.Chunks)
}

func (chunks *OrgChunks) clear(chunk Chunk, changes *ChunkChanges) {
	org := chunk.AsOrgChunk()
	next := chunks.Next[org.Id]
	prev := chunks.Prev[org.Id]
	if chunks.Prev[next] != "" {
		changes.addLink(next, "prev")
		delete(chunks.Prev, next)
	}
	if chunks.Prev[org.Id] != "" {
		changes.addLink(org.Id, "prev")
		delete(chunks.Prev, org.Id)
	}
	if chunks.Next[org.Id] != "" {
		changes.addLink(org.Id, "next")
		delete(chunks.Next, org.Id)
	}
	if chunks.Next[prev] != "" {
		changes.addLink(prev, "next")
		delete(chunks.Next, prev)
	}
	for _, child := range chunks.Children[org.Id] {
		changes.addLink(child, "parent")
		delete(chunks.Parent, child)
	}
	if chunks.Children[org.Id] != nil {
		changes.addLink(org.Id, "children")
		delete(chunks.Children, org.Id)
	}
	chunks.clearParent(org.Id, changes)
}

func (chunks *OrgChunks) clearParent(id OrgId, changes *ChunkChanges) {
	parent := chunks.Parent[id]
	if parent != "" {
		children := chunks.Children[parent]
		changes.addLink(parent, "children")
		if len(children) == 1 {
			chunks.Children[parent] = nil
		} else if len(children) > 1 {
			newChildren := make([]OrgId, 0, len(children)-1)
			for _, child := range children {
				if child != id {
					newChildren = append(newChildren, child)
				}
			}
			chunks.Children[parent] = newChildren
		}
		chunks.Parent[id] = ""
		changes.addLink(id, "parent")
	}
}

func (chunks *OrgChunks) addPending() {
	if chunks.PendingText.Len() > 0 {
		chunks.Chunks = chunks.Chunks.AddLast(chunks.newBasicChunk(TextType, chunks.PendingText.String()))
		chunks.PendingText.Reset()
	}
}

func (chunks *OrgChunks) add(ch Chunk) {
	chunks.addPending()
	chunks.Chunks = chunks.Chunks.AddLast(ch)
}

func (chunks *OrgChunks) nextId() OrgId {
	chunks.MaxId++
	return OrgId(fmt.Sprintf("chunk-%d", chunks.MaxId))
}

func (chunks *OrgChunks) newBasicChunk(tp OrgType, text string) *BasicChunk {
	chunk := &BasicChunk{
		Type: tp,
		Id:   chunks.nextId(),
		Text: text,
	}
	chunks.ChunkIds[chunk.Id] = chunk
	return chunk
}

func (chunks *OrgChunks) addBlock(tp OrgType, label, labelEnd int, line, rest string) string {
	sb := strings.Builder{}
	sb.WriteString(line)
	oldRest := rest
	pos := len(line)
	content := pos
	for rest != "" {
		line, rest = eatLine(rest)
		sb.WriteString(line)
		typ, fun, _ := chunks.lineType(line)
		if typ == tp && fun == nil {
			if tp == BlockType && htmlStartRE.MatchString(line) {
				tp = HtmlType
			}
			chunks.add(&Block{
				BasicChunk: *chunks.newBasicChunk(tp, sb.String()),
				Content:    content,
				End:        pos,
				Label:      label,
				LabelEnd:   labelEnd,
			})
			return rest
		} else if illegalBlockContent[typ] {
			break
		}
		pos += len(line)
	}
	return oldRest
}

func (typ OrgType) isSourcePrecursor() bool {
	return typ == SourceType || typ == TextType
}

func (chunks *OrgChunks) lineType(line string) (OrgType, func(m []int, line, rest string) string, []int) {
	if m := headlineRE.FindStringSubmatchIndex(line); len(m) > 0 {
		return HeadlineType, chunks.parseHeadline, m
	} else if m := drawerRE.FindStringSubmatchIndex(line); len(m) > 0 {
		return DrawerType, chunks.parseDrawer, m
	} else if m := drawerEndRE.FindStringSubmatchIndex(line); len(m) > 0 {
		return DrawerType, nil, m
	} else if m := srcStartRE.FindStringSubmatchIndex(line); len(m) > 0 {
		return SourceType, chunks.parseSource, m
	} else if m := srcEndRE.FindStringSubmatchIndex(line); len(m) > 0 {
		return SourceType, nil, m
	} else if m := blockStartRE.FindStringSubmatchIndex(line); len(m) > 0 {
		return BlockType, chunks.parseBlock, m
	} else if m := blockEndRE.FindStringSubmatchIndex(line); len(m) > 0 {
		return BlockType, nil, m
	} else if m := keywordRE.FindStringSubmatchIndex(line); len(m) > 0 {
		return KeywordType, chunks.parseKeyword, m
	} else if m := tableRowRE.FindStringSubmatchIndex(line); len(m) > 0 {
		return TableType, chunks.parseTable, m
	}
	return TextType, chunks.parseText, []int{0, len(line)}
}

func (chunks *OrgChunks) parseHeadline(m []int, line, rest string) string {
	chunks.add(&Headline{
		BasicChunk: *chunks.newBasicChunk(HeadlineType, line),
		Level:      m[3] - m[2],
	})
	return rest
}

func (chunks *OrgChunks) parseDrawer(m []int, line, rest string) string {
	if newRest := chunks.addBlock(DrawerType, m[2], m[3], line, rest); len(newRest) < len(rest) {
		return newRest
	}
	return line + rest
}

func (chunks *OrgChunks) parseSource(m []int, line, rest string) string {
	if newRest := chunks.addBlock(SourceType, m[2], m[3], line, rest); len(newRest) < len(rest) {
		// thanks to Soheil Hassas Yeganeh
		// https://groups.google.com/g/golang-nuts/c/pNwqLyfl2co/m/APaZSSvQUAAJ
		lastQuote := rune(0)
		f := func(c rune) bool {
			switch {
			case c == lastQuote:
				lastQuote = rune(0)
				return false
			case lastQuote != rune(0):
				return false
			case unicode.In(c, unicode.Quotation_Mark):
				lastQuote = c
				return false
			default:
				return unicode.IsSpace(c)
			}
		}
		blk, _ := chunks.Chunks.PeekLast().(*Block)
		chunks.Chunks = chunks.Chunks.RemoveLast()
		options := ([]string)(nil)
		value := (any)(nil)
		if m[4] >= 0 {
			options = strings.FieldsFunc(line[m[4]:m[5]], f)
		}
		var err error
		if content := strings.TrimSpace(blk.Text[blk.Content:blk.End]); content != "" {
			if language := strings.ToLower(blk.LabelText()); language == "json" {
				err = json.Unmarshal([]byte(content), &value)
			} else if language == "yaml" {
				err = yaml.Unmarshal([]byte(content), &value)
			} else if language == "toml" {
				err = toml.Unmarshal([]byte(content), &value)

			}
			if err != nil {
				value = nil
			}
		}
		chunks.add(&SourceBlock{
			Block:   *blk,
			Options: options,
			Value:   value,
		})
		return newRest
	}
	return line + rest
}

func (chunks *OrgChunks) parseBlock(m []int, line, rest string) string {
	nameStart := m[2]
	nameEnd := m[3]
	if nameStart < 0 {
		nameStart = m[4] - 1
		nameEnd = nameStart
	}
	if newRest := chunks.addBlock(BlockType, nameStart, nameEnd, line, rest); len(newRest) < len(rest) {
		return newRest
	}
	return line + rest
}

func (chunks *OrgChunks) parseKeyword(m []int, line, rest string) string {
	if m[4] != -1 && strings.ToLower(line[m[2]:m[3]]) == "name" {
		// if this name precedes a source block, parse the block instead
		sb := strings.Builder{}
		sb.WriteString(line)
		tmpLine, tmpRest := eatLine(rest)
		for tmpLine != "" && strings.TrimSpace(tmpLine) == "" {
			sb.WriteString(tmpLine)
			tmpLine, tmpRest = eatLine(tmpRest)
		}
		typ, _, blkM := chunks.lineType(tmpLine)
		if typ == SourceType {
			srcRest := chunks.parseSource(blkM, tmpLine, tmpRest)
			if len(srcRest) < len(tmpRest) {
				// source block successfully parsed -- patch name and intervening text into it
				src, _ := chunks.Chunks.PeekLast().(*SourceBlock)
				src.SrcStart = sb.Len()
				src.Content += src.SrcStart
				src.End += src.SrcStart
				src.Label += src.SrcStart
				src.LabelEnd += src.SrcStart
				src.NameStart = m[4]
				src.NameEnd = m[5]
				sb.WriteString(src.Text)
				src.Text = sb.String()
				return srcRest
			}
		} else if typ == TableType {
			tblRest := chunks.parseTable(blkM, tmpLine, tmpRest)
			if len(tblRest) < len(tmpRest) {
				tbl, _ := chunks.Chunks.PeekLast().(*TableBlock)
				tbl.TblStart = sb.Len()
				tbl.NameStart = m[4]
				tbl.NameEnd = m[5]
				sb.WriteString(tbl.Text)
				tbl.Text = sb.String()
				return tblRest
			}
		}
	}
	chunks.add(&Keyword{
		BasicChunk: *chunks.newBasicChunk(KeywordType, line),
		Label:      m[2],
		LabelEnd:   m[3],
		Content:    m[4],
	})
	return rest
}

func (chunks *OrgChunks) parseTable(m []int, line, rest string) string {
	rowValues := make([][]any, 0, 10)
	rowStrings := make([][]string, 0, 10)
	sb := strings.Builder{}
	maxLen := 0
	for {
		if typ, _, _ := chunks.lineType(line); typ == TableType {
			sb.WriteString(line)
			strs, values := parseTableRow(line)
			rowStrings = append(rowStrings, strs)
			rowValues = append(rowValues, values)
			line, rest = eatLine(rest)
			if maxLen < len(rowStrings) {
				maxLen = len(rowStrings)
			}
		} else {
			break
		}
	}
	// make it rectangular
	for i, strRow := range rowStrings {
		for len(strRow) < maxLen {
			rowStrings[i] = append(rowStrings[i], "")
			rowValues[i] = append(rowValues[i], nil)
		}
	}
	chunks.add(&TableBlock{
		BasicChunk: *chunks.newBasicChunk(TableType, sb.String()),
		Cells:      rowStrings,
		Value:      rowValues,
	})
	return rest
}

func parseTableRow(row string) ([]string, []any) {
	strs := make([]string, 0, 8)
	values := make([]any, 0, 8)
	for _, cell := range strings.Split(row, "|") {
		var cellValue any
		strs = append(strs, strings.TrimSpace(cell))
		if len(strs[len(strs)-1]) == 0 {
			values = append(values, nil)
		} else if err := json.Unmarshal([]byte(cell), &cellValue); err == nil {
			values = append(values, cellValue)
		} else {
			values = append(values, strings.TrimSpace(cell))
		}
	}
	return strs, values
}

func (chunks *OrgChunks) parseText(m []int, line, rest string) string {
	chunks.PendingText.WriteString(line)
	return rest
}

func (chunks *OrgChunks) parseChunk(line, rest string) string {
	_, fun, m := chunks.lineType(line)
	if fun != nil {
		if result := fun(m, line, rest); len(result) <= len(rest) {
			return result
		}
	}
	chunks.parseText(m, line, rest)
	return rest
}

func (chunks *OrgChunks) RelinkHierarchy(changes *ChunkChanges) {
	t := chunks.Chunks
	chunk := Chunk(nil)
	for !t.IsEmpty() {
		chunk, t = chunks.relinkChunk(t, 0, changes)
		verbose(1, "Clearing parent of %s\n", chunk.AsOrgChunk().Id)
		chunks.clearParent(chunk.AsOrgChunk().Id, changes)
	}
}

func (chunks *OrgChunks) relinkChunk(tree orgTree, level int, changes *ChunkChanges) (Chunk, orgTree) {
	if hl, ok := tree.PeekFirst().(*Headline); ok && hl.Level > level {
		tree = tree.RemoveFirst()
		for !tree.IsEmpty() {
			child, newRest := chunks.relinkChunk(tree, hl.Level, changes)
			if hl2, ok := child.(*Headline); ok && hl2.Level <= hl.Level {
				break
			}
			verbose(1, "Relinking parent of %s to %s\n", child.AsOrgChunk().Id, hl.AsOrgChunk().Id)
			if chunks.Parent[child.AsOrgChunk().Id] != hl.Id {
				chunks.clearParent(child.AsOrgChunk().Id, changes)
				chunks.link(child.AsOrgChunk().Id, "parent", hl.Id, changes)
			}
			tree = newRest
		}
		return hl, tree
	}
	return tree.PeekFirst(), tree.RemoveFirst()
}

func (chunks *OrgChunks) link(id OrgId, name string, value OrgId, changes *ChunkChanges) {
	switch name {
	case "next":
		if chunks.Next[id] == value {
			return
		}
		chunks.Next[id] = value
		chunks.Prev[value] = id
	case "parent":
		if chunks.Parent[id] == value {
			return
		}
		chunks.Parent[id] = value
		if chunks.Children[value] == nil {
			chunks.Children[value] = append(make([]OrgId, 0, 4), id)
		} else {
			chunks.Children[value] = append(chunks.Children[value], id)
		}
	}
	if changes != nil {
		changes.addLink(id, name)
		if name == "next" {
			changes.addLink(value, "prev")
		} else {
			changes.addLink(value, "children")
		}
	}
}

type ChunkChanges struct {
	Changed idSet
	Added   idSet
	Removed []OrgId
	Linked  map[OrgId]doc.Set[string]
}

func (ch *ChunkChanges) Order(chunks *OrgChunks) []OrgId {
	ids := make(idSet, len(ch.Changed)+len(ch.Added)+len(ch.Removed)+len(ch.Linked))
	ids.Merge(ch.Changed)
	ids.Merge(ch.Added)
	for _, id := range ch.Removed {
		ids.Add(id)
	}
	for id := range ch.Linked {
		ids.Add(id)
	}
	chunkList := make([]Chunk, 0, len(ids))
	for id := range ids {
		chunkList = append(chunkList, chunks.ChunkIds[id])
	}
	chunks.Sort(chunkList)
	result := make([]OrgId, 0, len(chunkList))
	for _, chunk := range chunkList {
		result = append(result, chunk.AsOrgChunk().Id)
	}
	return result
}

func (ch *ChunkChanges) DataChanges(chunks *OrgChunks) map[string]any {
	result := make(map[string]any, len(ch.Changed)+len(ch.Added))
	for id := range ch.Changed.Union(ch.Added) {
		if src, ok := chunks.ChunkIds[id].(*SourceBlock); ok && src.IsNamedData() {
			result[src.Name()] = src.Value
		} else if tbl, ok := chunks.ChunkIds[id].(*TableBlock); ok && tbl.Name() != "" {
			result[tbl.Name()] = tbl.Value
		}
	}
	return result
}

func (ch *ChunkChanges) IsEmpty() bool {
	return ch.Changed.IsEmpty() && ch.Added.IsEmpty() && len(ch.Linked) == 0 && len(ch.Added) == 0
}

func (ch *ChunkChanges) addLink(chunk OrgId, link string) {
	if ch != nil {
		if ch.Linked[chunk] == nil {
			if ch.Linked == nil {
				ch.Linked = make(map[OrgId]doc.Set[string], 4)
			}
			ch.Linked[chunk] = doc.NewSet(link)
		} else {
			ch.Linked[chunk].Add(link)
		}
	}
}

func (ch *ChunkChanges) Merge(more *ChunkChanges) {
	ch.Changed = ch.Changed.Union(more.Changed)
	ch.Added = ch.Added.Union(more.Added)
	if len(more.Linked) > 0 {
		if len(ch.Linked) == 0 {
			ch.Linked = map[OrgId]doc.Set[string]{}
		}
		for link := range more.Linked {
			ch.Linked[link] = ch.Linked[link].Union(more.Linked[link])
		}
	}
	s := doc.NewSet(ch.Removed...).Union(doc.NewSet(more.Removed...))
	ch.Removed = s.ToSlice()
}

func getText(t orgTree) string {
	sb := strings.Builder{}
	t.Each(func(item Chunk) bool {
		sb.WriteString(item.text())
		return true
	})
	return sb.String()
}

// returns changed blocks
func (chunks *OrgChunks) Replace(offset, len int, text string) *ChunkChanges {
	left, mid, right, newChunks := chunks.initialReplacement(offset, len, text)
	verbose(1, "TRIMMING CHUNKS\n")
	verbose(1, "  OLD:\n")
	DisplayChunks("    ", mid)
	verbose(1, "  NEW:\n")
	DisplayChunks("    ", newChunks)
	left, mid, right, newChunks = trimUnchangedChunks(left, mid, right, newChunks)
	changes := chunks.computeRemovesAndNewBlockIds(mid, newChunks)
	verbose(1, "  TRIMMED OLD:\n")
	DisplayChunks("    ", mid)
	verbose(1, "  TRIMMED NEW:\n")
	DisplayChunks("    ", newChunks)
	verbose(1, "NEW-CHUNKS: %+v\n", newChunks)
	verbose(1, "CHANGES: %+v\n", changes)
	newChunks.Each(func(chunk Chunk) bool {
		chunks.ChunkIds[chunk.AsOrgChunk().Id] = chunk
		return true
	})
	if !newChunks.IsEmpty() {
		// splice newChunks in and fix up next/prev links
		if left.IsEmpty() {
			left = left.AddLast(newChunks.PeekFirst())
			newChunks = newChunks.RemoveFirst()
		}
		changes.Linked = make(map[OrgId]doc.Set[string], newChunks.Measure().Count+2)
		prev := left.PeekLast().AsOrgChunk()
		newChunks.Each(func(chunk Chunk) bool {
			org := chunk.AsOrgChunk()
			chunks.link(prev.Id, "next", org.Id, changes)
			prev = org
			left = left.AddLast(chunk)
			return true
		})
		if !left.IsEmpty() && !right.IsEmpty() {
			leftId := left.PeekLast().AsOrgChunk().Id
			rightId := right.PeekFirst().AsOrgChunk().Id
			chunks.link(leftId, "next", rightId, changes)
		}
	}
	chunks.Chunks = left.Concat(right)
	return changes
}

// assign ids to new blocks and summarize changes
func (chunks *OrgChunks) computeRemovesAndNewBlockIds(old, new orgTree) *ChunkChanges {
	removed := []OrgId(nil)
	changes := &ChunkChanges{}
	if old.Measure().Count > new.Measure().Count {
		removed = make([]OrgId, 0, old.Measure().Count-new.Measure().Count)
		for i := old.Measure().Count - 1; i >= new.Measure().Count; i-- {
			oldOrg := old.PeekLast().AsOrgChunk()
			removed = append(removed, oldOrg.Id)
			chunks.clear(oldOrg, changes)
			old = old.RemoveLast()
		}
		if len(changes.Linked) > 0 {
			for _, id := range removed {
				delete(changes.Linked, id)
			}
			if len(changes.Linked) == 0 {
				changes.Linked = nil
			}
		}
	}
	changed := idSet(nil)
	if old.Measure().Count > 0 {
		changed = make(idSet, old.Measure().Count)
		old.Each(func(oldChunk Chunk) bool {
			oldOrg := oldChunk.AsOrgChunk()
			newChunk := new.PeekFirst()
			newChunk.AsOrgChunk().Id = oldOrg.Id
			new = new.RemoveFirst()
			changed.Add(oldOrg.Id)
			return true
		})
	}
	added := idSet(nil)
	if !new.IsEmpty() {
		added = make(idSet, new.Measure().Count)
		new.Each(func(newChunk Chunk) bool {
			id := chunks.nextId()
			newChunk.AsOrgChunk().Id = id
			added.Add(id)
			return true
		})
	}
	// up to this point, changes only has linked set
	changes.Changed = changed
	changes.Added = added
	changes.Removed = removed
	return changes
}

func escnl(str string) string {
	return strings.ReplaceAll(str, "\n", "\\n")
}

func treeText(tr orgTree) string {
	sb := strings.Builder{}
	for _, chunk := range tr.ToSlice() {
		fmt.Fprint(&sb, chunk.text())
	}
	return escnl(sb.String())
}

func (chunks *OrgChunks) initialReplacement(offset, len int, text string) (orgTree, orgTree, orgTree, orgTree) {
	// offset will lie within the first chunk of mid
	left, rest := chunks.Chunks.Split(func(m OrgMeasure) bool {
		return m.Width >= offset
	})
	adjLen := len + offset - left.Measure().Width
	// mid will contain almost all of the affected nodes
	// right will contain the last affected node
	mid, right := rest.Split(func(m OrgMeasure) bool {
		return m.Width >= adjLen
	})
	//DIAG
	str := treeText(chunks.Chunks)
	verbose(1, "@ REPLACE %d %d <%s>\n", offset, len, escnl(text))
	verbose(1, "@ left <%s>\n", escnl(str[:offset]))
	verbose(1, "@ mid <%s>\n", escnl(str[offset:offset+len]))
	verbose(1, "@ right <%s>\n", escnl(str[offset+len:]))
	verbose(1, "\n@ left chunks <%s>\n", treeText(left))
	verbose(1, "@ mid chunks <%s>\n", treeText(mid.AddLast(right.PeekFirst())))
	verbose(1, "@ right chunks <%s>\n", treeText(right.RemoveFirst()))
	for _, chunk := range mid.AddLast(right.PeekFirst()).ToSlice() {
		verbose(1, "%s\n", escnl(fmt.Sprintf("mid: %+v", chunk)))
	}
	diagsb := strings.Builder{}
	mid.AddLast(right.PeekFirst()).Each(func(chunk Chunk) bool {
		fmt.Fprintf(&diagsb, " %s", string(chunk.AsOrgChunk().Id))
		return true
	})
	verbose(1, "@ AFFECTING NODES:%s\n", diagsb.String())
	//END DIAG
	for i := 0; i < 2 && !right.IsEmpty(); i++ {
		mid = mid.AddLast(right.PeekFirst())
		right = right.RemoveFirst()
	}
	offset -= left.Measure().Width
	if !left.IsEmpty() {
		last := left.PeekLast()
		left = left.RemoveLast()
		mid = mid.AddFirst(last)
		offset += measure(last).Width
	}
	// catenate mid, replace, and parse
	txt := getText(mid)
	sb := strings.Builder{}
	sb.WriteString(txt[:offset])
	sb.WriteString(text)
	sb.WriteString(txt[offset+len:])
	return left, mid, right, Parse(sb.String()).Chunks
}

func DisplayChunks(prefix string, chunks orgTree) {
	if verbosity > 0 {
		chunks.Each(func(chunk Chunk) bool {
			verbose(1, "%s%s: <%s>\n", prefix, chunk.AsOrgChunk().Id,
				strings.ReplaceAll(chunk.text(), "\n", "\\n"))
			return true
		})
	}
}

func trimUnchangedChunks(left, mid, right, new orgTree) (orgTree, orgTree, orgTree, orgTree) {
	// trim unchanged blocks
	for !mid.IsEmpty() && !new.IsEmpty() && mid.PeekFirst().text() == new.PeekFirst().text() {
		left = left.AddLast(mid.PeekFirst())
		mid = mid.RemoveFirst()
		new = new.RemoveFirst()
	}
	for !mid.IsEmpty() && !new.IsEmpty() && mid.PeekLast().text() == new.PeekLast().text() {
		right = right.AddFirst(mid.PeekLast())
		mid = mid.RemoveLast()
		new = new.RemoveLast()
	}
	return left, mid, right, new
}
