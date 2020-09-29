// Code generated by "stringer -type=Token"; DO NOT EDIT.

package csql

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[ERROR-0]
	_ = x[SKIP-1]
	_ = x[EOF-2]
	_ = x[WS-3]
	_ = x[ASTERISK-4]
	_ = x[COMMA-5]
	_ = x[DOT-6]
	_ = x[LPAREN-7]
	_ = x[RPAREN-8]
	_ = x[LBRACKET-9]
	_ = x[RBRACKET-10]
	_ = x[EXCLAIM-11]
	_ = x[EQUALS-12]
	_ = x[LT-13]
	_ = x[GT-14]
	_ = x[PLUS-15]
	_ = x[MINUS-16]
	_ = x[SLASH-17]
	_ = x[PERCENT-18]
	_ = x[SEMICOLON-19]
	_ = x[SELECT-20]
	_ = x[DISTINCT-21]
	_ = x[COUNT-22]
	_ = x[SUM-23]
	_ = x[MAX-24]
	_ = x[MIN-25]
	_ = x[AVG-26]
	_ = x[AS-27]
	_ = x[FROM-28]
	_ = x[WHERE-29]
	_ = x[AND-30]
	_ = x[OR-31]
	_ = x[NOT-32]
	_ = x[IN-33]
	_ = x[IS-34]
	_ = x[BETWEEN-35]
	_ = x[WITHIN-36]
	_ = x[GROUP-37]
	_ = x[BY-38]
	_ = x[LIMIT-39]
	_ = x[NULL-40]
	_ = x[TRUE-41]
	_ = x[FALSE-42]
	_ = x[STRING-43]
	_ = x[NUMERIC-44]
	_ = x[IDENT-45]
}

const _Token_name = "ERRORSKIPEOFWSASTERISKCOMMADOTLPARENRPARENLBRACKETRBRACKETEXCLAIMEQUALSLTGTPLUSMINUSSLASHPERCENTSEMICOLONSELECTDISTINCTCOUNTSUMMAXMINAVGASFROMWHEREANDORNOTINISBETWEENWITHINGROUPBYLIMITNULLTRUEFALSESTRINGNUMERICIDENT"

var _Token_index = [...]uint8{0, 5, 9, 12, 14, 22, 27, 30, 36, 42, 50, 58, 65, 71, 73, 75, 79, 84, 89, 96, 105, 111, 119, 124, 127, 130, 133, 136, 138, 142, 147, 150, 152, 155, 157, 159, 166, 172, 177, 179, 184, 188, 192, 197, 203, 210, 215}

func (i Token) String() string {
	if i < 0 || i >= Token(len(_Token_index)-1) {
		return "Token(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _Token_name[_Token_index[i]:_Token_index[i+1]]
}
