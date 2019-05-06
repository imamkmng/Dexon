package checker

import (
	"bytes"
	"fmt"

	"github.com/dexon-foundation/decimal"

	"github.com/dexon-foundation/dexon/core/vm/sqlvm/ast"
	"github.com/dexon-foundation/dexon/core/vm/sqlvm/errors"
	"github.com/dexon-foundation/dexon/core/vm/sqlvm/schema"
)

func checkExpr(n ast.ExprNode,
	s schema.Schema, o CheckOptions, c *schemaCache, el *errors.ErrorList,
	tr schema.TableRef, ta typeAction) ast.ExprNode {

	switch n := n.(type) {
	case *ast.IdentifierNode:
		return checkVariable(n, s, o, c, el, tr, ta)

	case *ast.BoolValueNode:
		return checkBoolValue(n, o, el, ta)

	case *ast.AddressValueNode:
		return checkAddressValue(n, o, el, ta)

	case *ast.IntegerValueNode:
		return checkIntegerValue(n, o, el, ta)

	case *ast.DecimalValueNode:
		return checkDecimalValue(n, o, el, ta)

	case *ast.BytesValueNode:
		return checkBytesValue(n, o, el, ta)

	case *ast.NullValueNode:
		return checkNullValue(n, o, el, ta)

	case *ast.PosOperatorNode:
		return checkPosOperator(n, s, o, c, el, tr, ta)

	case *ast.NegOperatorNode:
		return checkNegOperator(n, s, o, c, el, tr, ta)

	case *ast.NotOperatorNode:
		return checkNotOperator(n, s, o, c, el, tr, ta)

	case *ast.ParenOperatorNode:
		return checkParenOperator(n, s, o, c, el, tr, ta)

	case *ast.AndOperatorNode:
		return checkAndOperator(n, s, o, c, el, tr, ta)

	case *ast.OrOperatorNode:
		return checkOrOperator(n, s, o, c, el, tr, ta)

	case *ast.GreaterOrEqualOperatorNode:
		return checkGreaterOrEqualOperator(n, s, o, c, el, tr, ta)

	case *ast.LessOrEqualOperatorNode:
		return checkLessOrEqualOperator(n, s, o, c, el, tr, ta)

	case *ast.NotEqualOperatorNode:
		return checkNotEqualOperator(n, s, o, c, el, tr, ta)

	case *ast.EqualOperatorNode:
		return checkEqualOperator(n, s, o, c, el, tr, ta)

	case *ast.GreaterOperatorNode:
		return checkGreaterOperator(n, s, o, c, el, tr, ta)

	case *ast.LessOperatorNode:
		return checkLessOperator(n, s, o, c, el, tr, ta)

	case *ast.ConcatOperatorNode:
		return checkConcatOperator(n, s, o, c, el, tr, ta)

	case *ast.AddOperatorNode:
		return n

	case *ast.SubOperatorNode:
		return n

	case *ast.MulOperatorNode:
		return n

	case *ast.DivOperatorNode:
		return n

	case *ast.ModOperatorNode:
		return n

	case *ast.IsOperatorNode:
		return n

	case *ast.LikeOperatorNode:
		return n

	case *ast.CastOperatorNode:
		return n

	case *ast.InOperatorNode:
		return n

	case *ast.FunctionOperatorNode:
		return n

	default:
		panic(fmt.Sprintf("unknown expression type %T", n))
	}
}

func elAppendTypeErrorAssignDataType(el *errors.ErrorList, n ast.ExprNode,
	fn string, dtExpected, dtGiven ast.DataType) {

	el.Append(errors.Error{
		Position: n.GetPosition(),
		Length:   n.GetLength(),
		Category: errors.ErrorCategorySemantic,
		Code:     errors.ErrorCodeTypeError,
		Severity: errors.ErrorSeverityError,
		Prefix:   fn,
		Message: fmt.Sprintf(
			"expect %s (%04x), but %s (%04x) is given",
			dtExpected.String(), uint16(dtExpected),
			dtGiven.String(), uint16(dtGiven)),
	}, nil)
}

// Procedure of type checking, type inference and constant folding.
//
// It is a reminder for developers who implement check functions for
// expressions. These steps are expected to be followed by all related
// functions. Steps which are not applicable can be skipped.
//
// 1. Call check functions for all child nodes.
//
// 2. Initialize the return value to the current node.
//    (1) There are two exceptions to this step, PosOperator and ParenOperator,
//        which are not recognized by the planner and the code generator. They
//        are basically no-ops and should be always removed.
//
// 3. Check data types for all child nodes.
//    (1) If the operator only operates on a limited set of data types, check
//        if all child nodes obey the restriction.
//    (2) If the operator requires all or some operands to have the same type,
//        check if corresponding child nodes meet the requirement. If these
//        operands include both nodes with types and node without types, check
//        and set types for nodes without types.
//    (3) Determine the data type of the current node.
//
// 4. Fold constants.
//    (1) Extract constant values stored in value nodes.
//    (2) Evaluate the expression and create a new node to hold the result of
//        the evaluation. Never modify a node in-place.
//    (3) Copy position, length, token from the current node to the new node.
//    (4) Set the data type of the new node to the one determined in 3-(3).
//    (5) Set the return value to the new node.
//
// 5. Process the type action.
//    (1) If the type action is nil, don't do anything.
//    (2) If the data type of the current node is still pending, determine the
//        type according to the type action.
//    (3) If the data type of the current node is already determined, don't
//        change the type. Instead, check if the current type is acceptable to
//        the type action if the type action is mandatory.

func checkVariable(n *ast.IdentifierNode,
	s schema.Schema, o CheckOptions, c *schemaCache, el *errors.ErrorList,
	tr schema.TableRef, ta typeAction) ast.ExprNode {

	fn := "CheckVariable"

	if (o & CheckWithConstantOnly) != 0 {
		el.Append(errors.Error{
			Position: n.GetPosition(),
			Length:   n.GetLength(),
			Category: errors.ErrorCategorySemantic,
			Code:     errors.ErrorCodeNonConstantExpression,
			Severity: errors.ErrorSeverityError,
			Prefix:   fn,
			Message: fmt.Sprintf("%s is not a constant",
				ast.QuoteIdentifier(n.Name)),
		}, nil)
		return nil
	}

	cn := string(n.Name)
	cd, found := c.FindColumnInBaseWithFallback(tr, cn, s)
	if !found {
		el.Append(errors.Error{
			Position: n.GetPosition(),
			Length:   n.GetLength(),
			Category: errors.ErrorCategorySemantic,
			Code:     errors.ErrorCodeColumnNotFound,
			Severity: errors.ErrorSeverityError,
			Prefix:   fn,
			Message: fmt.Sprintf(
				"cannot find column %s in table %s",
				ast.QuoteIdentifier(n.Name),
				ast.QuoteIdentifier(s[tr].Name)),
		}, nil)
		return nil
	}

	cr := cd.Column
	dt := s[tr].Columns[cr].Type
	switch a := ta.(type) {
	case typeActionInferDefault:
	case typeActionInferWithMajor:
	case typeActionInferWithSize:
	case typeActionAssign:
		if !dt.Equal(a.dt) {
			elAppendTypeErrorAssignDataType(el, n, fn, a.dt, dt)
			return nil
		}
	}

	n.SetType(dt)
	n.Desc = cd
	return n
}

func unknownValueNodeType(n ast.Valuer) string {
	return fmt.Sprintf("unknown constant type %T", n)
}

func describeValueNodeType(n ast.Valuer) string {
	switch n.(type) {
	case *ast.BoolValueNode:
		return "boolean constant"
	case *ast.AddressValueNode:
		return "address constant"
	case *ast.IntegerValueNode, *ast.DecimalValueNode:
		return "number constant"
	case *ast.BytesValueNode:
		return "bytes constant"
	case *ast.NullValueNode:
		return "null constant"
	default:
		panic(unknownValueNodeType(n))
	}
}

func elAppendTypeErrorAssignValueNode(el *errors.ErrorList, n ast.Valuer,
	fn string, dt ast.DataType) {

	el.Append(errors.Error{
		Position: n.GetPosition(),
		Length:   n.GetLength(),
		Category: errors.ErrorCategorySemantic,
		Code:     errors.ErrorCodeTypeError,
		Severity: errors.ErrorSeverityError,
		Prefix:   fn,
		Message: fmt.Sprintf(
			"expect %s (%04x), but %s is given",
			dt.String(), uint16(dt), describeValueNodeType(n)),
	}, nil)
}

func checkBoolValue(n *ast.BoolValueNode,
	o CheckOptions, el *errors.ErrorList, ta typeAction) ast.ExprNode {

	fn := "CheckBoolValue"

	switch a := ta.(type) {
	case typeActionInferDefault:
	case typeActionInferWithMajor:
	case typeActionInferWithSize:
	case typeActionAssign:
		major, _ := ast.DecomposeDataType(a.dt)
		if major != ast.DataTypeMajorBool {
			elAppendTypeErrorAssignValueNode(el, n, fn, a.dt)
			return nil
		}
	}
	return n
}

func checkAddressValue(n *ast.AddressValueNode,
	o CheckOptions, el *errors.ErrorList, ta typeAction) ast.ExprNode {

	fn := "CheckAddressValue"

	switch a := ta.(type) {
	case typeActionInferDefault:
	case typeActionInferWithMajor:
	case typeActionInferWithSize:
	case typeActionAssign:
		major, _ := ast.DecomposeDataType(a.dt)
		if major != ast.DataTypeMajorAddress {
			elAppendTypeErrorAssignValueNode(el, n, fn, a.dt)
			return nil
		}
	}
	return n
}

func mustGetMinMax(dt ast.DataType) (decimal.Decimal, decimal.Decimal) {
	min, max, ok := dt.GetMinMax()
	if !ok {
		panic(fmt.Sprintf("GetMinMax does not handle %v", dt))
	}
	return min, max
}

func mustDecimalEncode(dt ast.DataType, d decimal.Decimal) []byte {
	b, ok := ast.DecimalEncode(dt, d)
	if !ok {
		panic(fmt.Sprintf("DecimalEncode does not handle %v", dt))
	}
	return b
}

func mustDecimalDecode(dt ast.DataType, b []byte) decimal.Decimal {
	d, ok := ast.DecimalDecode(dt, b)
	if !ok {
		panic(fmt.Sprintf("DecimalDecode does not handle %v", dt))
	}
	return d
}

func cropDecimal(dt ast.DataType, d decimal.Decimal) decimal.Decimal {
	b := mustDecimalEncode(dt, d)
	return mustDecimalDecode(dt, b)
}

func elAppendConstantTooLongError(el *errors.ErrorList, n ast.Valuer,
	fn string, v decimal.Decimal) {

	el.Append(errors.Error{
		Position: n.GetPosition(),
		Length:   n.GetLength(),
		Category: errors.ErrorCategorySemantic,
		Code:     errors.ErrorCodeConstantTooLong,
		Severity: errors.ErrorSeverityError,
		Prefix:   fn,
		Message: fmt.Sprintf(
			"constant expression %s has more than %d digits",
			ast.QuoteString(n.GetToken()), MaxIntegerPartDigits),
	}, nil)
}

func elAppendOverflowError(el *errors.ErrorList, n ast.Valuer,
	fn string, dt ast.DataType, v, min, max decimal.Decimal) {

	el.Append(errors.Error{
		Position: n.GetPosition(),
		Length:   n.GetLength(),
		Category: errors.ErrorCategorySemantic,
		Code:     errors.ErrorCodeOverflow,
		Severity: errors.ErrorSeverityError,
		Prefix:   fn,
		Message: fmt.Sprintf(
			"number %s (%s) overflows %s (%04x)",
			ast.QuoteString(n.GetToken()), v.String(),
			dt.String(), uint16(dt)),
	}, nil)
	el.Append(errors.Error{
		Position: n.GetPosition(),
		Length:   n.GetLength(),
		Category: 0,
		Code:     0,
		Severity: errors.ErrorSeverityNote,
		Prefix:   fn,
		Message: fmt.Sprintf(
			"the range of %s is [%s, %s]",
			dt.String(), min.String(), max.String()),
	}, nil)
}

func elAppendOverflowWarning(el *errors.ErrorList, n ast.Valuer,
	fn string, dt ast.DataType, from, to decimal.Decimal) {

	el.Append(errors.Error{
		Position: n.GetPosition(),
		Length:   n.GetLength(),
		Category: 0,
		Code:     0,
		Severity: errors.ErrorSeverityWarning,
		Prefix:   fn,
		Message: fmt.Sprintf(
			"number %s (%s) overflows %s (%04x), converted to %s",
			ast.QuoteString(n.GetToken()), from.String(),
			dt.String(), uint16(dt), to.String()),
	}, nil)
}

func checkIntegerValue(n *ast.IntegerValueNode,
	o CheckOptions, el *errors.ErrorList, ta typeAction) ast.ExprNode {

	fn := "CheckIntegerValue"

	normalizeDecimal(&n.V)
	if !safeDecimalRange(n.V) {
		elAppendConstantTooLongError(el, n, fn, n.V)
		return nil
	}

	infer := func(size int) (ast.DataType, bool) {
		// The first case: assume V fits in the signed integer.
		minor := ast.DataTypeMinor(size - 1)
		dt := ast.ComposeDataType(ast.DataTypeMajorInt, minor)
		min, max := mustGetMinMax(dt)
		// Return if V < min. V must be negative so it must be signed.
		if n.V.LessThan(min) {
			if (o & CheckWithSafeMath) != 0 {
				elAppendOverflowError(el, n, fn, dt, n.V, min, max)
				return dt, false
			}
			cropped := cropDecimal(dt, n.V)
			elAppendOverflowWarning(el, n, fn, dt, n.V, cropped)
			normalizeDecimal(&cropped)
			n.V = cropped
			return dt, true
		}
		// We are done if V fits in the signed integer.
		if n.V.LessThanOrEqual(max) {
			return dt, true
		}

		// The second case: V is a non-negative integer, but it does not fit
		// in the signed integer. Test whether the unsigned integer works.
		dt = ast.ComposeDataType(ast.DataTypeMajorUint, minor)
		min, max = mustGetMinMax(dt)
		// Return if V > max. We don't have to test whether V < min because min
		// is always zero and we already know V is non-negative.
		if n.V.GreaterThan(max) {
			if (o & CheckWithSafeMath) != 0 {
				elAppendOverflowError(el, n, fn, dt, n.V, min, max)
				return dt, false
			}
			cropped := cropDecimal(dt, n.V)
			elAppendOverflowWarning(el, n, fn, dt, n.V, cropped)
			normalizeDecimal(&cropped)
			n.V = cropped
			return dt, true
		}
		return dt, true
	}

	dt := ast.DataTypePending
	switch a := ta.(type) {
	case typeActionInferDefault:
		// Default to int256 or uint256.
		var ok bool
		dt, ok = infer(256 / 8)
		if !ok {
			return nil
		}

	case typeActionInferWithSize:
		var ok bool
		dt, ok = infer(a.size)
		if !ok {
			return nil
		}

	case typeActionAssign:
		dt = a.dt
		major, _ := ast.DecomposeDataType(dt)
		switch {
		case major == ast.DataTypeMajorAddress:
			if !n.IsAddress {
				el.Append(errors.Error{
					Position: n.GetPosition(),
					Length:   n.GetLength(),
					Category: errors.ErrorCategorySemantic,
					Code:     errors.ErrorCodeInvalidAddressChecksum,
					Severity: errors.ErrorSeverityError,
					Prefix:   fn,
					Message: fmt.Sprintf(
						"expect %s (%04x), but %s is not an address constant",
						dt.String(), uint16(dt), n.GetToken()),
				}, nil)
				return nil
			}
			// Redirect to checkAddressValue if it becomes an address.
			addrNode := &ast.AddressValueNode{}
			addrNode.SetPosition(addrNode.GetPosition())
			addrNode.SetLength(addrNode.GetLength())
			addrNode.SetToken(addrNode.GetToken())
			addrNode.V = mustDecimalEncode(ast.ComposeDataType(
				ast.DataTypeMajorUint, ast.DataTypeMinor(160/8-1)), n.V)
			return checkAddressValue(addrNode, o, el, ta)

		case major == ast.DataTypeMajorInt,
			major == ast.DataTypeMajorUint,
			major.IsFixedRange(),
			major.IsUfixedRange():
			min, max := mustGetMinMax(dt)
			if n.V.LessThan(min) || n.V.GreaterThan(max) {
				if (o & CheckWithSafeMath) != 0 {
					elAppendOverflowError(el, n, fn, dt, n.V, min, max)
					return nil
				}
				cropped := cropDecimal(dt, n.V)
				elAppendOverflowWarning(el, n, fn, dt, n.V, cropped)
				normalizeDecimal(&cropped)
				n.V = cropped
			}

		default:
			elAppendTypeErrorAssignValueNode(el, n, fn, dt)
			return nil
		}
	}

	if !dt.Pending() {
		n.SetType(dt)
	}
	return n
}

func checkDecimalValue(n *ast.DecimalValueNode,
	o CheckOptions, el *errors.ErrorList, ta typeAction) ast.ExprNode {

	fn := "CheckDecimalValue"

	normalizeDecimal(&n.V)
	if !safeDecimalRange(n.V) {
		elAppendConstantTooLongError(el, n, fn, n.V)
		return nil
	}

	// Redirect to checkIntegerValue if the value is an integer.
	if intPart := n.V.Truncate(0); n.V.Equal(intPart) {
		intNode := &ast.IntegerValueNode{}
		intNode.SetPosition(n.GetPosition())
		intNode.SetLength(n.GetLength())
		intNode.SetToken(n.GetToken())
		intNode.SetType(n.GetType())
		intNode.IsAddress = false
		intNode.V = n.V
		return checkIntegerValue(intNode, o, el, ta)
	}

	infer := func(size, fractionalDigits int) (ast.DataType, bool) {
		// Infer the type in the samw way as checkIntegerValue.
		major := ast.DataTypeMajorFixed + ast.DataTypeMajor(size-1)
		minor := ast.DataTypeMinor(fractionalDigits)
		dt := ast.ComposeDataType(major, minor)
		min, max := mustGetMinMax(dt)
		if n.V.LessThan(min) {
			if (o & CheckWithSafeMath) != 0 {
				elAppendOverflowError(el, n, fn, dt, n.V, min, max)
				return dt, false
			}
			cropped := cropDecimal(dt, n.V)
			elAppendOverflowWarning(el, n, fn, dt, n.V, cropped)
			normalizeDecimal(&cropped)
			n.V = cropped
			return dt, false
		}
		if n.V.LessThanOrEqual(max) {
			return dt, true
		}

		major = ast.DataTypeMajorUfixed + ast.DataTypeMajor(size-1)
		minor = ast.DataTypeMinor(fractionalDigits)
		dt = ast.ComposeDataType(major, minor)
		min, max = mustGetMinMax(dt)
		if n.V.GreaterThan(max) {
			if (o & CheckWithSafeMath) != 0 {
				elAppendOverflowError(el, n, fn, dt, n.V, min, max)
				return dt, false
			}
			cropped := cropDecimal(dt, n.V)
			elAppendOverflowWarning(el, n, fn, dt, n.V, cropped)
			normalizeDecimal(&cropped)
			n.V = cropped
			return dt, true
		}
		return dt, true
	}

	// Now we are sure the number we are dealing has fractional part.
	dt := ast.DataTypePending
	switch a := ta.(type) {
	case typeActionInferDefault:
		// Default to fixed128x18 and ufixed128x18.
		var ok bool
		dt, ok = infer(128/8, 18)
		if !ok {
			return nil
		}

	case typeActionInferWithSize:
		// It is unclear that what the size hint means for fixed-point numbers,
		// so we just ignore it and do the same thing as the above case.
		var ok bool
		dt, ok = infer(128/8, 18)
		if !ok {
			return nil
		}

	case typeActionAssign:
		dt = a.dt
		major, _ := ast.DecomposeDataType(dt)
		switch {
		case major.IsFixedRange(),
			major.IsUfixedRange():
			min, max := mustGetMinMax(dt)
			if n.V.LessThan(min) || n.V.GreaterThan(max) {
				if (o & CheckWithSafeMath) != 0 {
					elAppendOverflowError(el, n, fn, dt, n.V, min, max)
					return nil
				}
				cropped := cropDecimal(dt, n.V)
				elAppendOverflowWarning(el, n, fn, dt, n.V, cropped)
				normalizeDecimal(&cropped)
				n.V = cropped
			}

		case major == ast.DataTypeMajorInt,
			major == ast.DataTypeMajorUint:
			el.Append(errors.Error{
				Position: n.GetPosition(),
				Length:   n.GetLength(),
				Category: errors.ErrorCategorySemantic,
				Code:     errors.ErrorCodeTypeError,
				Severity: errors.ErrorSeverityError,
				Prefix:   fn,
				Message: fmt.Sprintf(
					"expect %s (%04x), but the number %s has fractional part",
					dt.String(), uint16(dt), n.V.String()),
			}, nil)
			return nil

		default:
			elAppendTypeErrorAssignValueNode(el, n, fn, dt)
			return nil
		}
	}

	if !dt.Pending() {
		n.SetType(dt)
		_, minor := ast.DecomposeDataType(dt)
		fractionalDigits := int32(minor)
		n.V = n.V.Round(fractionalDigits)
		normalizeDecimal(&n.V)
	}
	return n
}

func checkBytesValue(n *ast.BytesValueNode,
	o CheckOptions, el *errors.ErrorList, ta typeAction) ast.ExprNode {

	fn := "CheckBytesValue"

	dt := ast.DataTypePending

executeTypeAction:
	switch a := ta.(type) {
	case typeActionInferDefault:
		// Default to bytes.
		major := ast.DataTypeMajorDynamicBytes
		minor := ast.DataTypeMinorDontCare
		dt = ast.ComposeDataType(major, minor)
		ta = newTypeActionAssign(dt)
		goto executeTypeAction

	case typeActionInferWithMajor:
		switch a.major {
		case ast.DataTypeMajorFixedBytes:
			if len(n.V) < 1 || len(n.V) > 32 {
				el.Append(errors.Error{
					Position: n.GetPosition(),
					Length:   n.GetLength(),
					Category: errors.ErrorCategorySemantic,
					Code:     errors.ErrorCodeTypeError,
					Severity: errors.ErrorSeverityError,
					Prefix:   fn,
					Message: fmt.Sprintf(
						"cannot infer %s (length %d) as fixed-size bytes",
						ast.QuoteString(n.V), len(n.V)),
				}, nil)
				return nil
			}
			minor := ast.DataTypeMinor(len(n.V))
			dt = ast.ComposeDataType(a.major, minor)
			ta = newTypeActionAssign(dt)
		case ast.DataTypeMajorDynamicBytes:
			dt = ast.ComposeDataType(a.major, ast.DataTypeMinorDontCare)
			ta = newTypeActionAssign(dt)
		default:
			ta = newTypeActionInferDefault()
		}
		goto executeTypeAction

	case typeActionInferWithSize:
		major := ast.DataTypeMajorFixedBytes
		minor := ast.DataTypeMinor(a.size - 1)
		dt = ast.ComposeDataType(major, minor)
		ta = newTypeActionAssign(dt)
		goto executeTypeAction

	case typeActionAssign:
		dt = a.dt
		major, minor := ast.DecomposeDataType(dt)
		switch major {
		case ast.DataTypeMajorDynamicBytes:
			// Do nothing because it is always valid.

		case ast.DataTypeMajorFixedBytes:
			sizeGiven := len(n.V)
			sizeExpected := int(minor) + 1
			if sizeGiven != sizeExpected {
				el.Append(errors.Error{
					Position: n.GetPosition(),
					Length:   n.GetLength(),
					Category: errors.ErrorCategorySemantic,
					Code:     errors.ErrorCodeTypeError,
					Severity: errors.ErrorSeverityError,
					Prefix:   fn,
					Message: fmt.Sprintf(
						"expect %s (%04x), but %s has %d bytes",
						dt.String(), uint16(dt),
						ast.QuoteString(n.V), sizeGiven),
				}, nil)
				return nil
			}

		default:
			elAppendTypeErrorAssignValueNode(el, n, fn, dt)
			return nil
		}
	}

	if !dt.Pending() {
		n.SetType(dt)
	}
	return n
}

func checkNullValue(n *ast.NullValueNode,
	o CheckOptions, el *errors.ErrorList, ta typeAction) ast.ExprNode {

	dt := ast.DataTypePending
	switch a := ta.(type) {
	case typeActionInferDefault:
		dt = ast.DataTypeNull
	case typeActionInferWithMajor:
		dt = ast.DataTypeNull
	case typeActionInferWithSize:
		dt = ast.DataTypeNull
	case typeActionAssign:
		dt = a.dt
	}

	if !dt.Pending() {
		n.SetType(dt)
	}
	return n
}

func elAppendTypeErrorOperatorValueNode(el *errors.ErrorList, n ast.Valuer,
	fn, op string) {

	el.Append(errors.Error{
		Position: n.GetPosition(),
		Length:   n.GetLength(),
		Category: errors.ErrorCategorySemantic,
		Code:     errors.ErrorCodeTypeError,
		Severity: errors.ErrorSeverityError,
		Prefix:   fn,
		Message: fmt.Sprintf("%s is not defined for %s",
			op, describeValueNodeType(n)),
	}, nil)
}

func elAppendTypeErrorOperatorDataType(el *errors.ErrorList, n ast.ExprNode,
	fn, op string, dt ast.DataType) {

	el.Append(errors.Error{
		Position: n.GetPosition(),
		Length:   n.GetLength(),
		Category: errors.ErrorCategorySemantic,
		Code:     errors.ErrorCodeTypeError,
		Severity: errors.ErrorSeverityError,
		Prefix:   fn,
		Message: fmt.Sprintf("%s is not defined for %s (%04x)",
			op, dt.String(), uint16(dt)),
	}, nil)
}

func validateNumberType(dt ast.DataType, el *errors.ErrorList, n ast.ExprNode,
	fn, op string) bool {

	if !dt.Pending() {
		major, _ := ast.DecomposeDataType(dt)
		switch {
		case major == ast.DataTypeMajorInt,
			major == ast.DataTypeMajorUint,
			major.IsFixedRange(),
			major.IsUfixedRange():
		default:
			elAppendTypeErrorOperatorDataType(el, n, fn, op, dt)
			return false
		}
	}
	return true
}

type extractNumberValueStatus uint8

const (
	extractNumberValueStatusError extractNumberValueStatus = iota
	extractNumberValueStatusInteger
	extractNumberValueStatusDecimal
	extractNumberValueStatusNullWithType
	extractNumberValueStatusNullWithoutType
)

func extractNumberValue(n ast.Valuer, el *errors.ErrorList,
	fn, op string) (decimal.Decimal, extractNumberValueStatus) {

	switch n := n.(type) {
	case *ast.IntegerValueNode:
		return n.V, extractNumberValueStatusInteger
	case *ast.DecimalValueNode:
		return n.V, extractNumberValueStatusDecimal
	case *ast.NullValueNode:
		if n.GetType().Pending() {
			return decimal.Zero, extractNumberValueStatusNullWithoutType
		}
		return decimal.Zero, extractNumberValueStatusNullWithType
	case *ast.BoolValueNode:
	case *ast.AddressValueNode:
	case *ast.BytesValueNode:
	default:
		panic(unknownValueNodeType(n))
	}
	elAppendTypeErrorOperatorValueNode(el, n, fn, op)
	return decimal.Zero, extractNumberValueStatusError
}

func checkPosOperator(n *ast.PosOperatorNode,
	s schema.Schema, o CheckOptions, c *schemaCache, el *errors.ErrorList,
	tr schema.TableRef, ta typeAction) ast.ExprNode {

	fn := "CheckPosOperator"
	op := "unary operator +"

	target := n.GetTarget()
	target = checkExpr(target, s, o, c, el, tr, nil)
	if target == nil {
		return nil
	}
	r := target

	dtTarget := target.GetType()
	if !validateNumberType(dtTarget, el, target, fn, op) {
		return nil
	}
	dt := dtTarget

	if target, ok := target.(ast.Valuer); ok {
		v, status := extractNumberValue(target, el, fn, op)
		switch status {
		case extractNumberValueStatusError:
			return nil
		case extractNumberValueStatusInteger:
			node := &ast.IntegerValueNode{}
			node.IsAddress = false
			node.V = v
			r = node
		case extractNumberValueStatusDecimal:
			node := &ast.DecimalValueNode{}
			node.V = v
			r = node
		case extractNumberValueStatusNullWithType:
			node := &ast.NullValueNode{}
			r = node
		case extractNumberValueStatusNullWithoutType:
			elAppendTypeErrorOperatorValueNode(el, target, fn, op)
			return nil
		default:
			panic(fmt.Sprintf("unknown status %d", status))
		}
		r.SetPosition(n.GetPosition())
		r.SetLength(n.GetLength())
		r.SetToken(n.GetToken())
		r.SetType(dt)
	}

	switch a := ta.(type) {
	case typeActionInferDefault:
		if dt.Pending() {
			r = checkExpr(r, s, o, c, el, tr, ta)
		}

	case typeActionInferWithMajor:
		if dt.Pending() {
			r = checkExpr(r, s, o, c, el, tr, ta)
		}

	case typeActionInferWithSize:
		if dt.Pending() {
			r = checkExpr(r, s, o, c, el, tr, ta)
		}

	case typeActionAssign:
		if dt.Pending() {
			r = checkExpr(r, s, o, c, el, tr, ta)
		} else {
			if !dt.Equal(a.dt) {
				elAppendTypeErrorAssignDataType(el, n, fn, a.dt, dt)
				return nil
			}
		}
	}
	return r
}

func checkNegOperator(n *ast.NegOperatorNode,
	s schema.Schema, o CheckOptions, c *schemaCache, el *errors.ErrorList,
	tr schema.TableRef, ta typeAction) ast.ExprNode {

	fn := "CheckNegOperator"
	op := "unary operator -"

	target := n.GetTarget()
	target = checkExpr(target, s, o, c, el, tr, nil)
	if target == nil {
		return nil
	}
	n.SetTarget(target)
	r := ast.ExprNode(n)

	dtTarget := target.GetType()
	if !validateNumberType(dtTarget, el, target, fn, op) {
		return nil
	}
	n.SetType(dtTarget)
	dt := n.GetType()

	eval := func(n ast.Valuer, v decimal.Decimal) (decimal.Decimal, bool) {
		r := v.Neg()
		if !dt.Pending() {
			min, max := mustGetMinMax(dt)
			if r.LessThan(min) || r.GreaterThan(max) {
				if (o & CheckWithSafeMath) != 0 {
					elAppendOverflowError(el, n, fn, dt, r, min, max)
					return r, false
				}
				cropped := cropDecimal(dt, r)
				elAppendOverflowWarning(el, n, fn, dt, r, cropped)
				r = cropped
			}
		}
		normalizeDecimal(&r)
		return r, true
	}
	if target, ok := target.(ast.Valuer); ok {
		v, status := extractNumberValue(target, el, fn, op)
		switch status {
		case extractNumberValueStatusError:
			return nil
		case extractNumberValueStatusInteger:
			node := &ast.IntegerValueNode{}
			node.IsAddress = false
			node.V, ok = eval(node, v)
			if !ok {
				return nil
			}
			r = node
		case extractNumberValueStatusDecimal:
			node := &ast.DecimalValueNode{}
			node.V, ok = eval(node, v)
			if !ok {
				return nil
			}
			r = node
		case extractNumberValueStatusNullWithType:
			node := &ast.NullValueNode{}
			r = node
		case extractNumberValueStatusNullWithoutType:
			elAppendTypeErrorOperatorValueNode(el, target, fn, op)
			return nil
		default:
			panic(fmt.Sprintf("unknown status %d", status))
		}
		r.SetPosition(n.GetPosition())
		r.SetLength(n.GetLength())
		r.SetToken(n.GetToken())
		r.SetType(dt)
	}

	switch a := ta.(type) {
	case typeActionInferDefault:
		if dt.Pending() {
			r = checkExpr(r, s, o, c, el, tr, ta)
		}

	case typeActionInferWithMajor:
		if dt.Pending() {
			r = checkExpr(r, s, o, c, el, tr, ta)
		}

	case typeActionInferWithSize:
		if dt.Pending() {
			r = checkExpr(r, s, o, c, el, tr, ta)
		}

	case typeActionAssign:
		if dt.Pending() {
			r = checkExpr(r, s, o, c, el, tr, ta)
		} else {
			if !dt.Equal(a.dt) {
				elAppendTypeErrorAssignDataType(el, n, fn, a.dt, dt)
				return nil
			}
		}
	}
	return r
}

func validateBoolType(dt ast.DataType, el *errors.ErrorList, n ast.ExprNode,
	fn, op string) bool {

	if !dt.Pending() {
		major, _ := ast.DecomposeDataType(dt)
		switch major {
		case ast.DataTypeMajorBool:
		default:
			elAppendTypeErrorOperatorDataType(el, n, fn, op, dt)
			return false
		}
	}
	return true
}

func extractBoolValue(n ast.Valuer, el *errors.ErrorList,
	fn, op string) (ast.BoolValue, bool) {

	switch n := n.(type) {
	case *ast.BoolValueNode:
		return n.V, true
	case *ast.NullValueNode:
		return ast.BoolValueUnknown, true
	case *ast.AddressValueNode:
	case *ast.IntegerValueNode:
	case *ast.DecimalValueNode:
	case *ast.BytesValueNode:
	default:
		panic(unknownValueNodeType(n))
	}
	elAppendTypeErrorOperatorValueNode(el, n, fn, op)
	return ast.BoolValueUnknown, false
}

func checkNotOperator(n *ast.NotOperatorNode,
	s schema.Schema, o CheckOptions, c *schemaCache, el *errors.ErrorList,
	tr schema.TableRef, ta typeAction) ast.ExprNode {

	fn := "CheckNotOperator"
	op := "unary operator NOT"

	target := n.GetTarget()
	target = checkExpr(target, s, o, c, el, tr, nil)
	if target == nil {
		return nil
	}
	n.SetTarget(target)
	r := ast.ExprNode(n)

	dtTarget := target.GetType()
	if !validateBoolType(dtTarget, el, target, fn, op) {
		return nil
	}
	dt := n.GetType()

	if target, ok := target.(ast.Valuer); ok {
		v, ok := extractBoolValue(target, el, fn, op)
		if !ok {
			return nil
		}
		node := &ast.BoolValueNode{}
		node.SetPosition(n.GetPosition())
		node.SetLength(n.GetLength())
		node.SetToken(n.GetToken())
		node.V = v.Not()
		r = node
	}

	switch a := ta.(type) {
	case typeActionInferDefault:
	case typeActionInferWithMajor:
	case typeActionInferWithSize:
	case typeActionAssign:
		if !dt.Equal(a.dt) {
			elAppendTypeErrorAssignDataType(el, n, fn, a.dt, dt)
			return nil
		}
	}
	return r
}

func checkParenOperator(n *ast.ParenOperatorNode,
	s schema.Schema, o CheckOptions, c *schemaCache, el *errors.ErrorList,
	tr schema.TableRef, ta typeAction) ast.ExprNode {

	r := n.GetTarget()
	r = checkExpr(r, s, o, c, el, tr, ta)
	r.SetPosition(n.GetPosition())
	r.SetLength(n.GetLength())
	r.SetToken(n.GetToken())
	return r
}

func checkAndOperator(n *ast.AndOperatorNode,
	s schema.Schema, o CheckOptions, c *schemaCache, el *errors.ErrorList,
	tr schema.TableRef, ta typeAction) ast.ExprNode {

	fn := "CheckAndOperator"
	op := "binary operator AND"

	object := n.GetObject()
	object = checkExpr(object, s, o, c, el, tr, nil)
	if object == nil {
		return nil
	}
	subject := n.GetSubject()
	subject = checkExpr(subject, s, o, c, el, tr, nil)
	if subject == nil {
		return nil
	}
	n.SetObject(object)
	n.SetSubject(subject)
	r := ast.ExprNode(n)

	dtObject := object.GetType()
	if !validateBoolType(dtObject, el, object, fn, op) {
		return nil
	}
	dtSubject := subject.GetType()
	if !validateBoolType(dtSubject, el, subject, fn, op) {
		return nil
	}
	dt := n.GetType()

	var v1, v2 ast.BoolValue
	if object, ok := object.(ast.Valuer); ok {
		if v1, ok = extractBoolValue(object, el, fn, op); !ok {
			return nil
		}
	}
	if subject, ok := subject.(ast.Valuer); ok {
		if v2, ok = extractBoolValue(subject, el, fn, op); !ok {
			return nil
		}
	}

	var vo ast.BoolValue
	switch {
	case v1.Valid() && v2.Valid():
		vo = v1.And(v2)
	case v1 == ast.BoolValueFalse || v2 == ast.BoolValueFalse:
		vo = ast.BoolValueFalse
	}
	if vo.Valid() {
		node := &ast.BoolValueNode{}
		node.SetPosition(n.GetPosition())
		node.SetLength(n.GetLength())
		node.SetToken(n.GetToken())
		node.V = vo
		r = node
	}

	switch a := ta.(type) {
	case typeActionInferDefault:
	case typeActionInferWithMajor:
	case typeActionInferWithSize:
	case typeActionAssign:
		if !dt.Equal(a.dt) {
			elAppendTypeErrorAssignDataType(el, n, fn, a.dt, dt)
			return nil
		}
	}
	return r
}

func checkOrOperator(n *ast.OrOperatorNode,
	s schema.Schema, o CheckOptions, c *schemaCache, el *errors.ErrorList,
	tr schema.TableRef, ta typeAction) ast.ExprNode {

	fn := "CheckOrOperator"
	op := "binary operator OR"

	object := n.GetObject()
	object = checkExpr(object, s, o, c, el, tr, nil)
	if object == nil {
		return nil
	}
	subject := n.GetSubject()
	subject = checkExpr(subject, s, o, c, el, tr, nil)
	if subject == nil {
		return nil
	}
	n.SetObject(object)
	n.SetSubject(subject)
	r := ast.ExprNode(n)

	dtObject := object.GetType()
	if !validateBoolType(dtObject, el, object, fn, op) {
		return nil
	}
	dtSubject := subject.GetType()
	if !validateBoolType(dtSubject, el, object, fn, op) {
		return nil
	}
	dt := n.GetType()

	var v1, v2 ast.BoolValue
	if object, ok := object.(ast.Valuer); ok {
		if v1, ok = extractBoolValue(object, el, fn, op); !ok {
			return nil
		}
	}
	if subject, ok := subject.(ast.Valuer); ok {
		if v2, ok = extractBoolValue(subject, el, fn, op); !ok {
			return nil
		}
	}

	var vo ast.BoolValue
	switch {
	case v1.Valid() && v2.Valid():
		vo = v1.Or(v2)
	case v1 == ast.BoolValueTrue || v2 == ast.BoolValueTrue:
		vo = ast.BoolValueTrue
	}
	if vo.Valid() {
		node := &ast.BoolValueNode{}
		node.SetPosition(n.GetPosition())
		node.SetLength(n.GetLength())
		node.SetToken(n.GetToken())
		node.V = vo
		r = node
	}

	switch a := ta.(type) {
	case typeActionInferDefault:
	case typeActionInferWithMajor:
	case typeActionInferWithSize:
	case typeActionAssign:
		if !dt.Equal(a.dt) {
			elAppendTypeErrorAssignDataType(el, n, fn, a.dt, dt)
			return nil
		}
	}
	return r
}

func validateOrderedType(dt ast.DataType, el *errors.ErrorList, n ast.ExprNode,
	fn, op string) bool {

	if !dt.Pending() {
		major, _ := ast.DecomposeDataType(dt)
		switch {
		case major == ast.DataTypeMajorBool,
			major == ast.DataTypeMajorAddress,
			major == ast.DataTypeMajorInt,
			major == ast.DataTypeMajorUint,
			major == ast.DataTypeMajorFixedBytes,
			major == ast.DataTypeMajorDynamicBytes,
			major.IsFixedRange(),
			major.IsFixedRange():
		default:
			elAppendTypeErrorOperatorDataType(el, n, fn, op, dt)
			return false
		}
	}
	return true
}

func elAppendTypeErrorOperandDataType(el *errors.ErrorList, n ast.ExprNode,
	fn, op string, dtExpected, dtGiven ast.DataType) {

	el.Append(errors.Error{
		Position: n.GetPosition(),
		Length:   n.GetLength(),
		Category: errors.ErrorCategorySemantic,
		Code:     errors.ErrorCodeTypeError,
		Severity: errors.ErrorSeverityError,
		Prefix:   fn,
		Message: fmt.Sprintf(
			"cannot use %s (%04x) with %s because the operand is expected "+
				"to be %s (%04x)",
			dtGiven.String(), uint16(dtGiven), op,
			dtExpected.String(), uint16(dtExpected)),
	}, nil)
}

func inferBinaryOperatorType(n ast.BinaryOperator,
	s schema.Schema, o CheckOptions, c *schemaCache, el *errors.ErrorList,
	tr schema.TableRef, fn, op string) (ast.DataType, bool) {

	object := n.GetObject()
	dtObject := object.GetType()
	dtObjectDetermined := !dtObject.Pending()

	subject := n.GetSubject()
	dtSubject := subject.GetType()
	dtSubjectDetermined := !dtSubject.Pending()

	switch {
	case dtObjectDetermined && dtSubjectDetermined:
		if !dtObject.Equal(dtSubject) {
			elAppendTypeErrorOperandDataType(
				el, subject, fn, op, dtObject, dtSubject)
			return ast.DataTypeBad, false
		}
		return dtObject, true

	case dtObjectDetermined && !dtSubjectDetermined:
		assign := newTypeActionAssign(dtObject)
		subject = checkExpr(subject, s, o, c, el, tr, assign)
		if subject == nil {
			return ast.DataTypeBad, false
		}
		n.SetSubject(subject)
		return dtObject, true

	case !dtObjectDetermined && dtSubjectDetermined:
		assign := newTypeActionAssign(dtSubject)
		object = checkExpr(object, s, o, c, el, tr, assign)
		if object == nil {
			return ast.DataTypeBad, false
		}
		n.SetObject(object)
		return dtSubject, true

	case !dtObjectDetermined && !dtSubjectDetermined:
		// We cannot do type checking when both types are unknown.
		return ast.DataTypePending, true

	default:
		panic("unreachable")
	}
}

func elAppendTypeErrorOperandValueNode(el *errors.ErrorList, n ast.Valuer,
	fn, op string, nExpected ast.Valuer) {

	el.Append(errors.Error{
		Position: n.GetPosition(),
		Length:   n.GetLength(),
		Category: errors.ErrorCategorySemantic,
		Code:     errors.ErrorCodeTypeError,
		Severity: errors.ErrorSeverityError,
		Prefix:   fn,
		Message: fmt.Sprintf(
			"cannot use %s with %s because the other operand is expected "+
				"to be %s",
			describeValueNodeType(n), op, describeValueNodeType(nExpected)),
	}, nil)
}

func extractConstantValue(n ast.Valuer) constantValue {
	switch n := n.(type) {
	case *ast.BoolValueNode:
		return newConstantValueBool(n.V)
	case *ast.AddressValueNode:
		return newConstantValueBytes(n.V)
	case *ast.IntegerValueNode:
		return newConstantValueDecimal(n.V)
	case *ast.DecimalValueNode:
		return newConstantValueDecimal(n.V)
	case *ast.BytesValueNode:
		return newConstantValueBytes(n.V)
	case *ast.NullValueNode:
		return nil
	default:
		panic(unknownValueNodeType(n))
	}
}

func unknownConstantValueType(v constantValue) string {
	return fmt.Sprintf("unknown constant value type %T", v)
}

func findNilConstantValue(v constantValue) constantValue {
	switch v.(type) {
	case constantValueBool:
		return newConstantValueBoolFromNil()
	case constantValueBytes:
		return newConstantValueBytesFromNil()
	case constantValueDecimal:
		return newConstantValueDecimalFromNil()
	case nil:
		return nil
	default:
		panic(unknownConstantValueType(v))
	}
}

func foldRelationalOperator(n ast.BinaryOperator, object, subject ast.Valuer,
	el *errors.ErrorList, fn, op string,
	evalBool func(ast.BoolValue, ast.BoolValue) ast.BoolValue,
	evalBytes func([]byte, []byte) ast.BoolValue,
	evalDecimal func(decimal.NullDecimal, decimal.NullDecimal) ast.BoolValue,
) *ast.BoolValueNode {

	compatibleTypes := func() bool {
		switch object.(type) {
		case *ast.BoolValueNode:
			switch subject.(type) {
			case *ast.BoolValueNode:
			case *ast.NullValueNode:
			default:
				return false
			}
		case *ast.AddressValueNode:
			switch subject.(type) {
			case *ast.AddressValueNode:
			case *ast.NullValueNode:
			default:
				return false
			}
		case *ast.IntegerValueNode:
			switch subject.(type) {
			case *ast.IntegerValueNode:
			case *ast.DecimalValueNode:
			case *ast.NullValueNode:
			default:
				return false
			}
		case *ast.DecimalValueNode:
			switch subject.(type) {
			case *ast.IntegerValueNode:
			case *ast.DecimalValueNode:
			case *ast.NullValueNode:
			default:
				return false
			}
		case *ast.BytesValueNode:
			switch subject.(type) {
			case *ast.BytesValueNode:
			case *ast.NullValueNode:
			default:
				return false
			}
		case *ast.NullValueNode:
		default:
			panic(unknownValueNodeType(object))
		}
		return true
	}

	if !compatibleTypes() {
		elAppendTypeErrorOperandValueNode(el, subject, fn, op, object)
		return nil
	}

	arg1 := extractConstantValue(object)
	arg2 := extractConstantValue(subject)

	// Resolve nil interfaces.
	if arg1 == nil && arg2 == nil {
		arg1 = newConstantValueBoolFromNil()
		arg2 = newConstantValueBoolFromNil()
	} else if arg1 == nil {
		arg1 = findNilConstantValue(arg2)
	} else if arg2 == nil {
		arg2 = findNilConstantValue(arg1)
	}

	// Now we are sure that all interfaces are non-nil.
	var vo ast.BoolValue
	switch v1 := arg1.(type) {
	case constantValueBool:
		v2 := arg2.(constantValueBool)
		vo = evalBool(v1.GetBool(), v2.GetBool())
	case constantValueBytes:
		v2 := arg2.(constantValueBytes)
		vo = evalBytes(v1.GetBytes(), v2.GetBytes())
	case constantValueDecimal:
		v2 := arg2.(constantValueDecimal)
		vo = evalDecimal(v1.GetDecimal(), v2.GetDecimal())
	default:
		panic(unknownConstantValueType(v1))
	}

	node := &ast.BoolValueNode{}
	node.SetPosition(n.GetPosition())
	node.SetLength(n.GetLength())
	node.SetToken(n.GetToken())
	node.V = vo
	return node
}

func checkRelationalOperator(n ast.BinaryOperator,
	s schema.Schema, o CheckOptions, c *schemaCache, el *errors.ErrorList,
	tr schema.TableRef, ta typeAction, fn, op string,
	evalBool func(ast.BoolValue, ast.BoolValue) ast.BoolValue,
	evalBytes func([]byte, []byte) ast.BoolValue,
	evalDecimal func(decimal.NullDecimal, decimal.NullDecimal) ast.BoolValue,
) ast.ExprNode {

	object := n.GetObject()
	object = checkExpr(object, s, o, c, el, tr, nil)
	if object == nil {
		return nil
	}
	subject := n.GetSubject()
	subject = checkExpr(subject, s, o, c, el, tr, nil)
	if subject == nil {
		return nil
	}
	n.SetObject(object)
	n.SetSubject(subject)
	r := ast.ExprNode(n)

	dtObject := object.GetType()
	if !validateOrderedType(dtObject, el, object, fn, op) {
		return nil
	}
	dtSubject := subject.GetType()
	if !validateOrderedType(dtSubject, el, subject, fn, op) {
		return nil
	}

	if _, ok := inferBinaryOperatorType(n, s, o, c, el, tr, fn, op); !ok {
		return nil
	}
	dt := n.GetType()

	if object, ok := object.(ast.Valuer); ok {
		if subject, ok := subject.(ast.Valuer); ok {
			node := foldRelationalOperator(n, object, subject, el, fn, op,
				evalBool, evalBytes, evalDecimal)
			if node == nil {
				return nil
			}
			r = node
		}
	}

	switch a := ta.(type) {
	case typeActionInferDefault:
	case typeActionInferWithMajor:
	case typeActionInferWithSize:
	case typeActionAssign:
		if !dt.Equal(a.dt) {
			elAppendTypeErrorAssignDataType(el, n, fn, a.dt, dt)
			return nil
		}
	}
	return r
}

func checkGreaterOrEqualOperator(n *ast.GreaterOrEqualOperatorNode,
	s schema.Schema, o CheckOptions, c *schemaCache, el *errors.ErrorList,
	tr schema.TableRef, ta typeAction) ast.ExprNode {

	fn := "CheckGreaterOrEqualOperator"
	op := "binary operator >="

	return checkRelationalOperator(n, s, o, c, el, tr, ta, fn, op,
		func(v1, v2 ast.BoolValue) ast.BoolValue {
			return v1.GreaterOrEqual(v2)
		},
		func(v1, v2 []byte) ast.BoolValue {
			if v1 == nil || v2 == nil {
				return ast.BoolValueUnknown
			}
			return ast.NewBoolValueFromBool(bytes.Compare(v1, v2) >= 0)
		},
		func(v1, v2 decimal.NullDecimal) ast.BoolValue {
			if !v1.Valid || !v2.Valid {
				return ast.BoolValueUnknown
			}
			return ast.NewBoolValueFromBool(
				v1.Decimal.GreaterThanOrEqual(v2.Decimal))
		},
	)
}

func checkLessOrEqualOperator(n *ast.LessOrEqualOperatorNode,
	s schema.Schema, o CheckOptions, c *schemaCache, el *errors.ErrorList,
	tr schema.TableRef, ta typeAction) ast.ExprNode {

	fn := "CheckLessOrEqualOperator"
	op := "binary operator <="

	return checkRelationalOperator(n, s, o, c, el, tr, ta, fn, op,
		func(v1, v2 ast.BoolValue) ast.BoolValue {
			return v1.LessOrEqual(v2)
		},
		func(v1, v2 []byte) ast.BoolValue {
			if v1 == nil || v2 == nil {
				return ast.BoolValueUnknown
			}
			return ast.NewBoolValueFromBool(bytes.Compare(v1, v2) <= 0)
		},
		func(v1, v2 decimal.NullDecimal) ast.BoolValue {
			if !v1.Valid || !v2.Valid {
				return ast.BoolValueUnknown
			}
			return ast.NewBoolValueFromBool(
				v1.Decimal.GreaterThanOrEqual(v2.Decimal))
		},
	)
}

func checkNotEqualOperator(n *ast.NotEqualOperatorNode,
	s schema.Schema, o CheckOptions, c *schemaCache, el *errors.ErrorList,
	tr schema.TableRef, ta typeAction) ast.ExprNode {

	fn := "CheckNotEqualOperator"
	op := "binary operator <>"

	return checkRelationalOperator(n, s, o, c, el, tr, ta, fn, op,
		func(v1, v2 ast.BoolValue) ast.BoolValue {
			return v1.NotEqual(v2)
		},
		func(v1, v2 []byte) ast.BoolValue {
			if v1 == nil || v2 == nil {
				return ast.BoolValueUnknown
			}
			return ast.NewBoolValueFromBool(!bytes.Equal(v1, v2))
		},
		func(v1, v2 decimal.NullDecimal) ast.BoolValue {
			if !v1.Valid || !v2.Valid {
				return ast.BoolValueUnknown
			}
			return ast.NewBoolValueFromBool(!v1.Decimal.Equal(v2.Decimal))
		},
	)
}

func checkEqualOperator(n *ast.EqualOperatorNode,
	s schema.Schema, o CheckOptions, c *schemaCache, el *errors.ErrorList,
	tr schema.TableRef, ta typeAction) ast.ExprNode {

	fn := "CheckEqualOperator"
	op := "binary operator ="

	return checkRelationalOperator(n, s, o, c, el, tr, ta, fn, op,
		func(v1, v2 ast.BoolValue) ast.BoolValue {
			return v1.Equal(v2)
		},
		func(v1, v2 []byte) ast.BoolValue {
			if v1 == nil || v2 == nil {
				return ast.BoolValueUnknown
			}
			return ast.NewBoolValueFromBool(bytes.Equal(v1, v2))
		},
		func(v1, v2 decimal.NullDecimal) ast.BoolValue {
			if !v1.Valid || !v2.Valid {
				return ast.BoolValueUnknown
			}
			return ast.NewBoolValueFromBool(v1.Decimal.Equal(v2.Decimal))
		},
	)
}

func checkGreaterOperator(n *ast.GreaterOperatorNode,
	s schema.Schema, o CheckOptions, c *schemaCache, el *errors.ErrorList,
	tr schema.TableRef, ta typeAction) ast.ExprNode {

	fn := "CheckGreaterOperator"
	op := "binary operator >"

	return checkRelationalOperator(n, s, o, c, el, tr, ta, fn, op,
		func(v1, v2 ast.BoolValue) ast.BoolValue {
			return v1.Greater(v2)
		},
		func(v1, v2 []byte) ast.BoolValue {
			if v1 == nil || v2 == nil {
				return ast.BoolValueUnknown
			}
			return ast.NewBoolValueFromBool(bytes.Compare(v1, v2) > 0)
		},
		func(v1, v2 decimal.NullDecimal) ast.BoolValue {
			if !v1.Valid || !v2.Valid {
				return ast.BoolValueUnknown
			}
			return ast.NewBoolValueFromBool(v1.Decimal.GreaterThan(v2.Decimal))
		},
	)
}

func checkLessOperator(n *ast.LessOperatorNode,
	s schema.Schema, o CheckOptions, c *schemaCache, el *errors.ErrorList,
	tr schema.TableRef, ta typeAction) ast.ExprNode {

	fn := "CheckLessOperator"
	op := "binary operator <"

	return checkRelationalOperator(n, s, o, c, el, tr, ta, fn, op,
		func(v1, v2 ast.BoolValue) ast.BoolValue {
			return v1.Greater(v2)
		},
		func(v1, v2 []byte) ast.BoolValue {
			if v1 == nil || v2 == nil {
				return ast.BoolValueUnknown
			}
			return ast.NewBoolValueFromBool(bytes.Compare(v1, v2) < 0)
		},
		func(v1, v2 decimal.NullDecimal) ast.BoolValue {
			if !v1.Valid || !v2.Valid {
				return ast.BoolValueUnknown
			}
			return ast.NewBoolValueFromBool(v1.Decimal.LessThan(v2.Decimal))
		},
	)
}

func validateBytesType(dt ast.DataType, el *errors.ErrorList, n ast.ExprNode,
	fn, op string) bool {

	if !dt.Pending() {
		major, _ := ast.DecomposeDataType(dt)
		switch major {
		case ast.DataTypeMajorFixedBytes,
			ast.DataTypeMajorDynamicBytes:
		default:
			elAppendTypeErrorOperatorDataType(el, n, fn, op, dt)
			return false
		}
	}
	return true
}

type extractBytesValueStatus uint8

const (
	extractBytesValueStatusError extractBytesValueStatus = iota
	extractBytesValueStatusBytes
	extractBytesValueStatusNullWithType
	extractBytesValueStatusNullWithoutType
)

func extractBytesValue(n ast.Valuer, el *errors.ErrorList,
	fn, op string) ([]byte, extractBytesValueStatus) {

	switch n := n.(type) {
	case *ast.BytesValueNode:
		return n.V, extractBytesValueStatusBytes
	case *ast.NullValueNode:
		if n.GetType().Pending() {
			return nil, extractBytesValueStatusNullWithoutType
		}
		return nil, extractBytesValueStatusNullWithType
	case *ast.BoolValueNode:
	case *ast.AddressValueNode:
	case *ast.IntegerValueNode:
	case *ast.DecimalValueNode:
	default:
		panic(unknownValueNodeType(n))
	}
	elAppendTypeErrorOperatorValueNode(el, n, fn, op)
	return nil, extractBytesValueStatusError
}

func checkConcatOperator(n *ast.ConcatOperatorNode,
	s schema.Schema, o CheckOptions, c *schemaCache, el *errors.ErrorList,
	tr schema.TableRef, ta typeAction) ast.ExprNode {

	fn := "CheckConcatOperator"
	op := "binary operator ||"

	object := n.GetObject()
	object = checkExpr(object, s, o, c, el, tr, nil)
	if object == nil {
		return nil
	}
	subject := n.GetSubject()
	subject = checkExpr(subject, s, o, c, el, tr, nil)
	if subject == nil {
		return nil
	}
	n.SetObject(object)
	n.SetSubject(subject)
	r := ast.ExprNode(n)

	dtObject := object.GetType()
	if !validateBytesType(dtObject, el, object, fn, op) {
		return nil
	}
	dtSubject := subject.GetType()
	if !validateBytesType(dtSubject, el, subject, fn, op) {
		return nil
	}

	dtObjectDetermined := !dtObject.Pending()
	dtSubjectDetermined := !dtSubject.Pending()

	// We cannot use inferBinaryOperatorType because we allows two sides of the
	// operator to have different types.
	unknownBytesMajor := func(major ast.DataTypeMajor) string {
		return fmt.Sprintf("%02x is not a bytes type", uint8(major))
	}
	describeBytesMajor := func(major ast.DataTypeMajor) string {
		switch major {
		case ast.DataTypeMajorFixedBytes:
			return "fixed-size"
		case ast.DataTypeMajorDynamicBytes:
			return "dynamically-sized"
		default:
			panic(unknownBytesMajor(major))
		}
	}
	reportMismatch := func(n ast.ExprNode, major1, major2 ast.DataTypeMajor) {
		el.Append(errors.Error{
			Position: n.GetPosition(),
			Length:   n.GetLength(),
			Category: errors.ErrorCategorySemantic,
			Code:     errors.ErrorCodeTypeError,
			Severity: errors.ErrorSeverityError,
			Prefix:   fn,
			Message: fmt.Sprintf("cannot use %s between %s and %s bytes", op,
				describeBytesMajor(major1), describeBytesMajor(major2)),
		}, nil)
	}
	reportTooBig := func(dt1, dt2 ast.DataType) {
		el.Append(errors.Error{
			Position: n.GetPosition(),
			Length:   n.GetLength(),
			Category: errors.ErrorCategorySemantic,
			Code:     errors.ErrorCodeTypeError,
			Severity: errors.ErrorSeverityError,
			Prefix:   fn,
			Message: fmt.Sprintf(
				"cannot use %s between %s (%04x) and %s (%04x) because "+
					"the result will be longer than 32 bytes",
				op, dt1.String(), uint16(dt1), dt2.String(), uint16(dt2)),
		}, nil)
		el.Append(errors.Error{
			Position: n.GetPosition(),
			Length:   n.GetLength(),
			Category: 0,
			Code:     0,
			Severity: errors.ErrorSeverityNote,
			Prefix:   fn,
			Message: fmt.Sprintf(
				"convert both arguments to %s bytes in order to "+
					"produce a binary string that is bigger than a slot",
				describeBytesMajor(ast.DataTypeMajorDynamicBytes)),
		}, nil)
	}
	updateObject := func() {
		n.SetObject(object)
		dtObject = object.GetType()
		dtObjectDetermined = !dtObject.Pending()
	}
	updateSubject := func() {
		n.SetSubject(subject)
		dtSubject = subject.GetType()
		dtSubjectDetermined = !dtSubject.Pending()
	}
	infer := func() (ast.DataType, bool) {
		if !dtObjectDetermined {
			panic("dtObject is pending")
		}
		if !dtSubjectDetermined {
			panic("dtSubject is pending")
		}
		majorObject, minorObject := ast.DecomposeDataType(dtObject)
		majorSubject, minorSubject := ast.DecomposeDataType(dtSubject)
		if majorObject != majorSubject {
			reportMismatch(n, majorObject, majorSubject)
			return ast.DataTypeBad, false
		}
		switch majorObject {
		case ast.DataTypeMajorFixedBytes:
			sizeObject := int(minorObject) + 1
			sizeSubject := int(minorSubject) + 1
			sizeOperator := sizeObject + sizeSubject
			if sizeOperator > 32 {
				reportTooBig(dtObject, dtSubject)
				return ast.DataTypeBad, false
			}
			majorOperator := ast.DataTypeMajorFixedBytes
			minorOperator := ast.DataTypeMinor(sizeOperator - 1)
			return ast.ComposeDataType(majorOperator, minorOperator), true
		case ast.DataTypeMajorDynamicBytes:
			return dtObject, true
		default:
			panic(unknownBytesMajor(majorObject))
		}
	}

	switch {
	case dtObjectDetermined && dtSubjectDetermined:
		dt, ok := infer()
		if !ok {
			return nil
		}
		n.SetType(dt)

	case dtObjectDetermined && !dtSubjectDetermined:
		var action typeAction
		major, _ := ast.DecomposeDataType(dtObject)
		switch major {
		case ast.DataTypeMajorFixedBytes:
			action = newTypeActionInferWithMajor(ast.DataTypeMajorFixedBytes)
		case ast.DataTypeMajorDynamicBytes:
			action = newTypeActionAssign(dtObject)
		default:
			panic(unknownBytesMajor(major))
		}
		subject = checkExpr(subject, s, o, c, el, tr, action)
		if subject == nil {
			return nil
		}
		updateSubject()
		dt, ok := infer()
		if !ok {
			return nil
		}
		n.SetType(dt)

	case !dtObjectDetermined && dtSubjectDetermined:
		var action typeAction
		major, _ := ast.DecomposeDataType(dtSubject)
		switch major {
		case ast.DataTypeMajorFixedBytes:
			action = newTypeActionInferWithMajor(ast.DataTypeMajorFixedBytes)
		case ast.DataTypeMajorDynamicBytes:
			action = newTypeActionAssign(dtSubject)
		default:
			panic(unknownBytesMajor(major))
		}
		object = checkExpr(object, s, o, c, el, tr, action)
		if object == nil {
			return nil
		}
		updateObject()
		dt, ok := infer()
		if !ok {
			return nil
		}
		n.SetType(dt)

	case !dtObjectDetermined && !dtSubjectDetermined:
		// Keep it undetermined if both sides are pending.

	default:
		panic("unreachable")
	}
	dt := n.GetType()

	if object, ok := object.(ast.Valuer); ok {
		if subject, ok := subject.(ast.Valuer); ok {
			null := false
			v1, status := extractBytesValue(object, el, fn, op)
			switch status {
			case extractBytesValueStatusError:
				return nil
			case extractBytesValueStatusBytes:
			case extractBytesValueStatusNullWithType:
				null = true
			case extractBytesValueStatusNullWithoutType:
				elAppendTypeErrorOperatorValueNode(el, object, fn, op)
				return nil
			default:
				panic(fmt.Sprintf("unknown status %d", status))
			}
			v2, status := extractBytesValue(subject, el, fn, op)
			switch status {
			case extractBytesValueStatusError:
				return nil
			case extractBytesValueStatusBytes:
			case extractBytesValueStatusNullWithType:
				null = true
			case extractBytesValueStatusNullWithoutType:
				elAppendTypeErrorOperatorValueNode(el, subject, fn, op)
				return nil
			default:
				panic(fmt.Sprintf("unknown status %d", status))
			}
			if null {
				node := &ast.NullValueNode{}
				r = node
			} else {
				node := &ast.BytesValueNode{}
				node.V = make([]byte, 0, len(v1)+len(v2))
				node.V = append(node.V, v1...)
				node.V = append(node.V, v2...)
				r = node
			}
			r.SetPosition(n.GetPosition())
			r.SetLength(n.GetLength())
			r.SetToken(n.GetToken())
			r.SetType(dt)
		}
	}

	switch a := ta.(type) {
	case typeActionInferDefault:
		if dt.Pending() {
			r = checkExpr(r, s, o, c, el, tr, ta)
		}

	case typeActionInferWithMajor:
		if dt.Pending() {
			r = checkExpr(r, s, o, c, el, tr, ta)
		}

	case typeActionInferWithSize:
		if dt.Pending() {
			r = checkExpr(r, s, o, c, el, tr, ta)
		}

	case typeActionAssign:
		if dt.Pending() {
			r = checkExpr(r, s, o, c, el, tr, ta)
		} else {
			if !dt.Equal(a.dt) {
				elAppendTypeErrorAssignDataType(el, n, fn, a.dt, dt)
				return nil
			}
		}
	}
	return r
}
