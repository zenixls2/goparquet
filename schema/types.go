package schema

import (
	"github.com/zenixls2/goparquet/ptype"
	"unsafe"
)

type DecimalMetadata struct {
	Isset     bool
	Scale     int32
	Precision int32
}

type ColumnPath struct {
	Path []string
}

type NodeType int

const (
	Node_PRIMITIVE NodeType = 0
	Node_GROUP     NodeType = 1
)

type Node struct {
	_type       NodeType
	name        string
	repetition  ptype.Repetition
	logicalType ptype.LogicalType
	id          int
	parent      *Node
}

func NewNode(_type NodeType, name string, repetition ptype.Repetition,
	params ...int) *Node {
	logicalType := ptype.LogicalType_NONE
	if len(params) > 0 {
		logicalType = (ptype.LogicalType)(params[0])
	}
	id := -1
	if len(params) > 1 {
		id = params[1]
	}
	return &Node{
		_type:       _type,
		name:        name,
		repetition:  repetition,
		logicalType: logicalType,
		id:          id,
		parent:      nil,
	}
}

func (n *Node) IsPrimitive() bool {
	return n._type == Node_PRIMITIVE
}

func (n *Node) IsGroup() bool {
	return n._type == Node_GROUP
}

func (n *Node) IsOptional() bool {
	return n.repetition == ptype.Repetition_OPTIONAL
}

func (n *Node) IsRepeated() bool {
	return n.repetition == ptype.Repetition_REPEATED
}

func (n *Node) IsRequired() bool {
	return n.repetition == ptype.Repetition_REQUIRED
}

func (n *Node) Equals(other *Node) bool {
	return false
}

func (n *Node) Name() string {
	return n.name
}

func (n *Node) NodeType() NodeType {
	return n._type
}

func (n *Node) Repetition() ptype.Repetition {
	return n.repetition
}

func (n *Node) LogicalType() ptype.LogicalType {
	return n.logicalType
}

func (n *Node) Id() int {
	return n.id
}

func (n *Node) Parent() *Node {
	return n.parent
}

func (n *Node) ToParquet(opaqueElement interface{}) {
}

type NodeVisitor struct{}

func (nv *NodeVisitor) Visit(node *Node) {}

type NodeConstVisitor struct{}

func (ncv *NodeConstVisitor) Visit(node *Node) {}

func (n *Node) Visit(visitor *NodeVisitor) {}

func (n *Node) VisitConst(visitor *NodeConstVisitor) {}

func (n *Node) EqualsInternal(other *Node) bool {
	return true
}

func (n *Node) SetParent(pParent *Node) {}

type PrimitiveNode struct {
	Node
	physicalType    ptype.Type
	typeLength      int32
	decimalMetadata DecimalMetadata
}

func PrimitiveNodeFromParquet(opaqueElement interface{}, id int) *Node {
	return &Node{}
}

func PrimitiveNodeMake(name string, repetition ptype.Repetition, _type ptype.Type,
	params ...int) *Node {
	logicalType := ptype.LogicalType_NONE
	if len(params) > 0 {
		logicalType = (ptype.LogicalType)(params[0])
	}
	length := -1
	if len(params) > 1 {
		length = params[1]
	}
	precision := -1
	if len(params) > 2 {
		precision = params[2]
	}
	scale := -1
	if len(params) > 3 {
		scale = params[3]
	}
	return (*Node)(unsafe.Pointer(NewPrimitiveNode(name, repetition, _type, int(logicalType), length, precision, scale)))
}

func Boolean(name string, repetition ptype.Repetition) *Node {
	return PrimitiveNodeMake(name, repetition, ptype.Type_BOOLEAN)
}

func Int32(name string, repetition ptype.Repetition) *Node {
	return PrimitiveNodeMake(name, repetition, ptype.Type_INT32)
}

func Int64(name string, repetition ptype.Repetition) *Node {
	return PrimitiveNodeMake(name, repetition, ptype.Type_INT64)
}

func Int96(name string, repetition ptype.Repetition) *Node {
	return PrimitiveNodeMake(name, repetition, ptype.Type_INT96)
}

func Float(name string, repetition ptype.Repetition) *Node {
	return PrimitiveNodeMake(name, repetition, ptype.Type_FLOAT)
}

func Double(name string, repetition ptype.Repetition) *Node {
	return PrimitiveNodeMake(name, repetition, ptype.Type_DOUBLE)
}

func ByteArray(name string, repetition ptype.Repetition) *Node {
	return PrimitiveNodeMake(name, repetition, ptype.Type_BYTE_ARRAY)
}

func (pn *PrimitiveNode) Equals(other *Node) bool {
	return true
}

func (pn *PrimitiveNode) PhysicalType() ptype.Type {
	return pn.physicalType
}

func (pn *PrimitiveNode) TypeLength() int32 {
	return pn.typeLength
}

func (pn *PrimitiveNode) DecimalMetadata() DecimalMetadata {
	return pn.decimalMetadata
}

func (pn *PrimitiveNode) ToParquet(opaqueElement interface{}) {
}

func (pn *PrimitiveNode) Visit(visitor *NodeVisitor) {
}

func (pn *PrimitiveNode) VisitConst(visitor *NodeConstVisitor) {
}

func NewPrimitiveNode(name string, repetition ptype.Repetition, _type ptype.Type,
	params ...int) *PrimitiveNode {
	logicalType := ptype.LogicalType_NONE
	if len(params) > 0 {
		logicalType = ptype.LogicalType(params[0])
	}
	length := -1
	if len(params) > 1 {
		length = params[1]
	}
	precision := -1
	if len(params) > 2 {
		precision = params[2]
	}
	scale := -1
	if len(params) > 3 {
		scale = params[3]
	}
	id := -1
	if len(params) > 4 {
		id = params[4]
	}
	return &PrimitiveNode{}
}

// For FIXED_LEN_BYTE_ARRAY
func (pn *PrimitiveNode) SetTypeLength(length int32) {
	pn.typeLength = length
}

// For Decimal logical type: Precision and scale
func (pn *PrimitiveNode) SetDecimalMetadata(scale, precision int32) {
	pn.decimalMetadata.Scale = scale
	pn.decimalMetadata.Precision = precision
}

func (pn *PrimitiveNode) EqualsInternal(other *PrimitiveNode) bool {
	return true
}

type GroupNode struct {
	Node
	fields []Node
}

func GroupNodeFromParquet(opaqueElement interface{}, id int, fields []Node) *Node {
	return &Node{}
}

func GroupNodeMake(name string, repetition ptype.Repetition, fields []Node, params ...int) *Node {
	logicalType := ptype.LogicalType_NONE
	if len(params) > 0 {
		logicalType = ptype.LogicalType(params[0])
	}
	return (*Node)(unsafe.Pointer(
		NewGroupNode(name, repetition, fields, int(logicalType))))
}

func (gn *GroupNode) Equals(other *Node) bool {
	return true
}

func (gn *GroupNode) Field(i int) *Node {
	return &gn.fields[i]
}

func (gn *GroupNode) FieldCount() int {
	return len(gn.fields)
}

func (gn *GroupNode) ToParquet(opaqueElement interface{}) {
}

func (gn *GroupNode) Visit(visitor *NodeVisitor) {
}

func (gn *GroupNode) VisitConst(visitor *NodeConstVisitor) {
}

func NewGroupNode(name string, repetition ptype.Repetition, fields []Node,
	params ...int) *GroupNode {
	logicalType := ptype.LogicalType_NONE
	if len(params) > 0 {
		logicalType = ptype.LogicalType(params[0])
	}
	id := -1
	if len(params) > 1 {
		id = params[1]
	}
	result := &GroupNode{
		Node: *NewNode(
			Node_GROUP,
			name,
			repetition,
			int(logicalType),
			id,
		),
		fields: fields,
	}
	for i := range fields {
		fields[i].SetParent((*Node)(unsafe.Pointer(result)))
	}
	return result
}

func (gn *GroupNode) EqualsInternal(other *GroupNode) bool {
	return true
}
