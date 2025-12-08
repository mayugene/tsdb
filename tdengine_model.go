package tsdb

type TdengineHttpOutput struct {
	Code       int      `json:"code"`
	ColumnMeta [][3]any `json:"column_meta"`
	Data       [][]any  `json:"data"`
	Rows       int      `json:"rows"`
}
