package schema

import (
	"fmt"
	"unsafe"
)

type SchemaElement struct {
}

type SchemaDescriptor struct {
}

type FlatSchemaConverter struct {
	Elements  []SchemaElement
	Length    int
	Pos       int
	CurrentId int
}

func (f *FlatSchemaConverter) Convert() *Node {
	root := &f.Elements[0]

	// Validate the root node
	if root.NumChildren == 0 {
		panic(fmt.Errorf("Root node did not have children"))
	}

	return f.NextNode()
}

func (f *FlatSchemaConverter) NextId() int {
	tmp := f.CurrentId
	f.CurrentId++
	return tmp
}

func (f *FlatSchemaConverter) NextNode() *Node {
	element := f.Next()
	nodeId := f.NextId()
	opaqueElement := element
	if element.NumChildren == 0 {
		// Leaf (primitive node)
		return PrimitiveNodeFromParquet(opaqueElement, nodeId)
	} else {
		// Group
		var fields []Node
		for i := 0; i < element.NumChildren; i++ {
			field := f.NextNode()
			fields = append(fields, *field)
		}
		return GroupNodeFromParquet(opaqueElement, nodeId, fields)
	}
}

func (f *FlatSchemaConverter) Next() *SchemaElement {
	if f.Pos == f.Length {
		panic(fmt.Errorf("Malformed schema: not enough SchemaElement values"))
	}
	pos := f.Pos
	f.Pos++
	return &f.Elements[pos]
}

func FromParquet(schema []SchemaElement) *SchemaDescriptor {
	converter := NewFlatSchemaConverter(&schema[0], len(schema))
	root := converter.Convert()
	descr := &SchemaDescriptor{}
	descr.Init(root.Release())

	return descr
}

func ToParquet(schema *GroupNode, out []SchemaElement) {
	flattener := NewSchemaFlattener(schema, out)
	flattener.Flatten()
}

type SchemaVisitor struct {
	NodeConstVisitor
	Elements []SchemaElement
}

func (sv *SchemaVisitor) Visit(node *Node) {
	element := SchemaElement{}
	node.ToParquet(&element)
	// Override fieldId here as we can get user-generated Nodes without a valid id
	element.SetFieldId(len(sv.Elements))
	sv.Elements = append(sv.Elements, element)

	if node.IsGroup() {
		groupNode := (*GroupNode)(unsafe.Pointer(node))
		for i := 0; i < groupNode.FieldCount(); i++ {
			groupNode.Field(i).VisitConst((*NodeConstVisitor)(unsafe.Pointer(sv)))
		}
	}
}

func NewSchemaVisitor(elements []SchemaElement) *SchemaVisitor {
	return &SchemaVisitor{
		Elements: elements,
	}
}

type SchemaFlattener struct {
	Elements []SchemaElement
}

func NewSchemaFlattener(schema *GroupNode, out []SchemaElement) *SchemaFlattener {
	sf := SchemaFlattener{}
	sf.Root = schema
	sf.Elements = out
	return &sf
}
