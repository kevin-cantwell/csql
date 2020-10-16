package csql

type Executor struct {
	Stmt *Statement
	DB   *Database
}

// func (e *Executor) Stream(ctx context.Context) <-chan Event {

// }

// func (e *Executor) subscribeFrom(ctx context.Context, from *FromClause) []<-chan Event {
// 	var subs []<-chan Event
// 	var expr *TablesExpression
// 	expr = &from.Tables
// 	for expr != nil {
// 		idents := splitIdent(expr.Ident.Raw)
// 		var table string
// 		switch len(idents) {
// 		case 1:
// 			subs = append(subs, e.DB.Subscribe(ctx, table))
// 		}
// 		expr = expr.CrossJoin
// 	}
// 	return subs
// }
