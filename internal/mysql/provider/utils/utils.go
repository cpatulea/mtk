package utils

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/skpr/mtk/internal/mysql/provider"
)

// QueryColumnsForTable for a given table.
func QueryColumnsForTable(database *sql.DB, table string, params provider.DumpParams) ([]string, error) {
	var rows *sql.Rows

	rows, err := database.Query(fmt.Sprintf("SELECT * FROM `%s` LIMIT 1", table))
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	columnsMap := make(map[string]struct{})

	for k, column := range columns {
		columnsMap[strings.ToLower(column)] = struct{}{}
		replacement, ok := params.SelectMap[strings.ToLower(table)][strings.ToLower(column)]
		if ok {
			columns[k] = fmt.Sprintf("%s AS `%s`", replacement, column)
		} else {
			columns[k] = fmt.Sprintf("`%s`", column)
		}
	}

	// Check rewrite rules are valid
	for column, _ := range params.SelectMap[strings.ToLower(table)] {
		_, ok := columnsMap[strings.ToLower(column)]
		if !ok {
			panic(fmt.Sprintf("rewrite rule references non-existent column: %s", column))
		}
	}

	return columns, nil
}
