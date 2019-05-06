package checker

import (
	"github.com/dexon-foundation/dexon/core/vm/sqlvm/ast"
	"github.com/dexon-foundation/dexon/core/vm/sqlvm/errors"
	"github.com/dexon-foundation/dexon/core/vm/sqlvm/schema"
)

func checkSelectStmt(n *ast.SelectStmtNode, s schema.Schema,
	o CheckOptions, c *schemaCache, el *errors.ErrorList) {
}

func checkUpdateStmt(n *ast.UpdateStmtNode, s schema.Schema,
	o CheckOptions, c *schemaCache, el *errors.ErrorList) {
}

func checkDeleteStmt(n *ast.DeleteStmtNode, s schema.Schema,
	o CheckOptions, c *schemaCache, el *errors.ErrorList) {
}

func checkInsertStmt(n *ast.InsertStmtNode, s schema.Schema,
	o CheckOptions, c *schemaCache, el *errors.ErrorList) {
}
