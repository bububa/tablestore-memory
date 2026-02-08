package tablestore

import (
	"errors"
	"fmt"
	"slices"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search"
	"github.com/golang/protobuf/proto"

	"github.com/bububa/tablestore-memory/model"
)

func (s *MemoryStore) InitSessionTable() error {
	listResp, err := s.clt.ListTable()
	if err != nil {
		return fmt.Errorf("list session table failed during init session table, %w", err)
	}
	if slices.Contains(listResp.TableNames, s.SessionTableName) {
		describeReq := new(tablestore.DescribeTableRequest)
		describeReq.TableName = s.SessionTableName
		describeResp, err := s.clt.DescribeTable(describeReq)
		if err != nil {
			return fmt.Errorf("describe session table failed during init session table, %w", err)
		}
		// Check if secondary index exists
		indexExists := false
		for _, v := range describeResp.IndexMetas {
			if v.IndexName == s.SessionSecondaryIndexName {
				indexExists = true
				break
			}
		}

		// Only create index if it doesn't exist
		if !indexExists {
			indexMeta := new(tablestore.IndexMeta)
			indexMeta.IndexName = s.SessionSecondaryIndexName
			indexMeta.AddPrimaryKeyColumn(SessionUserIDField)
			indexMeta.AddPrimaryKeyColumn(SessionUpdateTimeField)
			indexMeta.AddPrimaryKeyColumn(SessionSessionIDField)
			indexMeta.SetAsLocalIndex()
			createIndexReq := new(tablestore.CreateIndexRequest)
			createIndexReq.MainTableName = s.SessionTableName
			createIndexReq.IndexMeta = indexMeta
			if _, err := s.clt.CreateIndex(createIndexReq); err != nil {
				return fmt.Errorf("create session table secondary index failed during init session table, %w", err)
			}
		}
		searchIndexExists := false
		listSearchIndexReq := new(tablestore.ListSearchIndexRequest)
		listSearchIndexReq.TableName = s.SessionTableName
		indexResp, err := s.clt.ListSearchIndex(listSearchIndexReq)
		if err != nil {
			return fmt.Errorf("list session search index failed during init session table, %w", err)
		}
		for _, v := range indexResp.IndexInfo {
			if v.IndexName == s.SessionSearchIndexName {
				searchIndexExists = true
				break
			}
		}

		if !searchIndexExists {
			if err := s.createSessionSearchIndex(); err != nil {
				return fmt.Errorf("create session table search index failed during init session table, %w", err)
			}
		}
		return nil
	}
	tableMeta := new(tablestore.TableMeta)
	tableMeta.TableName = s.SessionTableName
	tableMeta.AddPrimaryKeyColumn(SessionUserIDField, tablestore.PrimaryKeyType_STRING)
	tableMeta.AddPrimaryKeyColumn(SessionSessionIDField, tablestore.PrimaryKeyType_STRING)
	tableMeta.AddDefinedColumn(SessionUpdateTimeField, tablestore.DefinedColumn_INTEGER)
	tableOption := new(tablestore.TableOption)
	tableOption.MaxVersion = 1
	tableOption.TimeToAlive = -1
	reservedThroughput := new(tablestore.ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0
	indexMeta := new(tablestore.IndexMeta)
	indexMeta.IndexName = s.SessionSecondaryIndexName
	indexMeta.AddPrimaryKeyColumn(SessionUserIDField)
	indexMeta.AddPrimaryKeyColumn(SessionUpdateTimeField)
	indexMeta.AddPrimaryKeyColumn(SessionSessionIDField)
	indexMeta.SetAsLocalIndex()
	createTableRequest := new(tablestore.CreateTableRequest)
	createTableRequest.TableMeta = tableMeta
	createTableRequest.TableOption = tableOption
	createTableRequest.ReservedThroughput = reservedThroughput
	createTableRequest.AddIndexMeta(indexMeta)
	if _, err := s.clt.CreateTable(createTableRequest); err != nil {
		return fmt.Errorf("create session table failed, %w", err)
	}
	if err := s.createSessionSearchIndex(); err != nil {
		return fmt.Errorf("create session table search index failed during init session table, %w", err)
	}
	return nil
}

func (s *MemoryStore) createSessionSearchIndex() error {
	analyzer := tablestore.Analyzer_Fuzzy
	createReq := new(tablestore.CreateSearchIndexRequest)
	createReq.TableName = s.SessionTableName
	createReq.IndexName = s.SessionSearchIndexName
	createReq.IndexSchema = &tablestore.IndexSchema{
		FieldSchemas: []*tablestore.FieldSchema{
			{
				FieldName: proto.String(SessionUserIDField),
				FieldType: tablestore.FieldType_KEYWORD,
				Index:     proto.Bool(true),
			},
			{
				FieldName: proto.String(SessionUpdateTimeField),
				FieldType: tablestore.FieldType_LONG,
				Index:     proto.Bool(true),
			},
			{
				FieldName: proto.String(SessionSearchContentField),
				FieldType: tablestore.FieldType_TEXT,
				Index:     proto.Bool(true),
				Analyzer:  &analyzer,
				AnalyzerParameter: tablestore.FuzzyAnalyzerParameter{
					MinChars: 1,
					MaxChars: 7,
				},
			},
		},
	}
	_, err := s.clt.CreateSearchIndex(createReq)
	if err != nil {
		return fmt.Errorf("create session search index failed, %w", err)
	}
	return nil
}

func (s *MemoryStore) PutSession(session *model.Session) error {
	pk := new(tablestore.PrimaryKey)
	pk.AddPrimaryKeyColumn(SessionUserIDField, session.UserID)
	pk.AddPrimaryKeyColumn(SessionSessionIDField, session.SessionID)
	putReq := new(tablestore.PutRowRequest)
	putReq.PutRowChange = new(tablestore.PutRowChange)
	putReq.PutRowChange.TableName = s.SessionTableName
	putReq.PutRowChange.PrimaryKey = pk
	putReq.PutRowChange.AddColumn(SessionUpdateTimeField, session.UpdateTime)
	if session.SearchContent != "" {
		putReq.PutRowChange.AddColumn(SessionSearchContentField, session.SearchContent)
	}
	for k, v := range session.Metadata {
		putReq.PutRowChange.AddColumn(k, v)
	}
	putReq.PutRowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
	if _, err := s.clt.PutRow(putReq); err != nil {
		return fmt.Errorf("put session to memory store failed, %w", err)
	}
	return nil
}

func (s *MemoryStore) UpdateSession(session *model.Session) error {
	tmp := model.Session{
		UserID:    session.UserID,
		SessionID: session.SessionID,
	}
	if err := s.GetSession(&tmp); err != nil {
		return fmt.Errorf("update session failed, %w", err)
	}
	pk := new(tablestore.PrimaryKey)
	pk.AddPrimaryKeyColumn(SessionUserIDField, session.UserID)
	pk.AddPrimaryKeyColumn(SessionSessionIDField, session.SessionID)
	updateReq := new(tablestore.UpdateRowRequest)
	updateReq.UpdateRowChange = new(tablestore.UpdateRowChange)
	updateReq.UpdateRowChange.TableName = s.SessionTableName
	updateReq.UpdateRowChange.PrimaryKey = pk
	updateReq.UpdateRowChange.PutColumn(SessionUpdateTimeField, session.UpdateTime)
	if session.SearchContent != "" {
		updateReq.UpdateRowChange.PutColumn(SessionSearchContentField, session.SearchContent)
	} else {
		updateReq.UpdateRowChange.DeleteColumn(SessionSearchContentField)
	}
	for k, v := range session.Metadata {
		updateReq.UpdateRowChange.PutColumn(k, v)
	}
	for k := range tmp.Metadata {
		if _, ok := session.Metadata[k]; !ok {
			updateReq.UpdateRowChange.DeleteColumn(k)
		}
	}
	updateReq.UpdateRowChange.SetCondition(tablestore.RowExistenceExpectation_EXPECT_EXIST)
	if _, err := s.clt.UpdateRow(updateReq); err != nil {
		return fmt.Errorf("update session in memory store failed, %w", err)
	}
	return nil
}

func (s *MemoryStore) DeleteSession(userID string, sessionID string) error {
	pk := new(tablestore.PrimaryKey)
	pk.AddPrimaryKeyColumn(SessionUserIDField, userID)
	pk.AddPrimaryKeyColumn(SessionSessionIDField, sessionID)
	deleteReq := new(tablestore.DeleteRowRequest)
	deleteReq.DeleteRowChange = new(tablestore.DeleteRowChange)
	deleteReq.DeleteRowChange.TableName = s.SessionTableName
	deleteReq.DeleteRowChange.PrimaryKey = pk
	deleteReq.DeleteRowChange.SetCondition(tablestore.RowExistenceExpectation_EXPECT_EXIST)
	if _, err := s.clt.DeleteRow(deleteReq); err != nil {
		return fmt.Errorf("delete session in memory store failed, %w", err)
	}
	return nil
}

func (s *MemoryStore) DeleteSessions(userID string) (int, error) {
	list := s.ListSessions(userID, nil, -1, 5000)
	var (
		count int
		total int
	)
	// Process items in batches of 200
	currentBatch := new(tablestore.BatchWriteRowRequest)
	for v := range list {
		rowChange := new(tablestore.DeleteRowChange)
		rowChange.TableName = s.SessionTableName
		rowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
		pk := new(tablestore.PrimaryKey)
		pk.AddPrimaryKeyColumn(SessionUserIDField, v.UserID)
		pk.AddPrimaryKeyColumn(SessionSessionIDField, v.SessionID)
		rowChange.PrimaryKey = pk
		currentBatch.AddRowChange(rowChange)
		count++

		if count >= 200 {
			if _, err := s.clt.BatchWriteRow(currentBatch); err != nil {
				return total, fmt.Errorf("delete user sessions failed, %w", err)
			}
			total += count
			count = 0
			// Create a new batch request for next batch
			currentBatch = new(tablestore.BatchWriteRowRequest)
		}
	}
	if count > 0 {
		if _, err := s.clt.BatchWriteRow(currentBatch); err != nil {
			return total, fmt.Errorf("delete user sessions failed, %w", err)
		}
		total += count
	}
	return total, nil
}

func (s *MemoryStore) DeleteAllSessions() (int, error) {
	list := s.ListAllSessions()
	var (
		count int
		total int
	)
	// Process items in batches of 200
	currentBatch := new(tablestore.BatchWriteRowRequest)
	for v := range list {
		rowChange := new(tablestore.DeleteRowChange)
		rowChange.TableName = s.SessionTableName
		rowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
		pk := new(tablestore.PrimaryKey)
		pk.AddPrimaryKeyColumn(SessionUserIDField, v.UserID)
		pk.AddPrimaryKeyColumn(SessionSessionIDField, v.SessionID)
		rowChange.PrimaryKey = pk
		currentBatch.AddRowChange(rowChange)
		count++

		if count >= 200 {
			if _, err := s.clt.BatchWriteRow(currentBatch); err != nil {
				return total, fmt.Errorf("delete user sessions failed, %w", err)
			}
			total += count
			count = 0
			// Create a new batch request for next batch
			currentBatch = new(tablestore.BatchWriteRowRequest)
		}
	}
	if count > 0 {
		if _, err := s.clt.BatchWriteRow(currentBatch); err != nil {
			return total, fmt.Errorf("delete user sessions failed, %w", err)
		}
		total += count
	}
	return total, nil
}

func (s *MemoryStore) GetSession(session *model.Session) error {
	pk := new(tablestore.PrimaryKey)
	pk.AddPrimaryKeyColumn(SessionUserIDField, session.UserID)
	pk.AddPrimaryKeyColumn(SessionSessionIDField, session.SessionID)
	getReq := new(tablestore.GetRowRequest)
	getReq.SingleRowQueryCriteria = new(tablestore.SingleRowQueryCriteria)
	getReq.SingleRowQueryCriteria.TableName = s.SessionTableName
	getReq.SingleRowQueryCriteria.PrimaryKey = pk
	getReq.SingleRowQueryCriteria.MaxVersion = 1
	resp, err := s.clt.GetRow(getReq)
	if err != nil {
		return fmt.Errorf("failed to get session in memory store, %w", err)
	}
	if len(resp.PrimaryKey.PrimaryKeys) == 0 {
		return fmt.Errorf("session not exists")
	}
	parseSessionFromRow(session, resp.Columns, &resp.PrimaryKey)
	return nil
}

func (s *MemoryStore) ListAllSessions() <-chan model.Session {
	return s.ListSessions("", nil, -1, 5000)
}

func (s *MemoryStore) ListSessions(userID string, filter tablestore.ColumnFilter, maxCount int, batchSize int) <-chan model.Session {
	startPk := new(tablestore.PrimaryKey)
	if userID != "" {
		startPk.AddPrimaryKeyColumn(SessionUserIDField, userID)
	} else {
		startPk.AddPrimaryKeyColumnWithMinValue(SessionUserIDField)
	}
	startPk.AddPrimaryKeyColumnWithMinValue(SessionSessionIDField)
	endPk := new(tablestore.PrimaryKey)
	if userID != "" {
		endPk.AddPrimaryKeyColumn(SessionUserIDField, userID)
	} else {
		endPk.AddPrimaryKeyColumnWithMaxValue(SessionUserIDField)
	}
	endPk.AddPrimaryKeyColumnWithMaxValue(SessionSessionIDField)
	criteria := new(tablestore.RangeRowQueryCriteria)
	criteria.TableName = s.SessionTableName
	criteria.StartPrimaryKey = startPk
	criteria.EndPrimaryKey = endPk
	criteria.Direction = tablestore.FORWARD
	criteria.MaxVersion = 1
	if filter != nil {
		criteria.Filter = filter
	}
	if maxCount <= 0 {
		maxCount = -1
	}
	criteria.Limit = int32(configBatchSize(batchSize, maxCount, filter))
	rangeReq := new(tablestore.GetRangeRequest)
	rangeReq.RangeRowQueryCriteria = criteria
	retCh := make(chan model.Session)
	go func() {
		defer close(retCh)
		resp, err := s.clt.GetRange(rangeReq)
		if err != nil {
			// Log error but can't return it since we're in a goroutine
			// In a real implementation, consider using context cancellation or error channels
			return
		}
		var count int
		for _, row := range resp.Rows {
			var session model.Session
			parseSessionFromRow(&session, row.Columns, row.PrimaryKey)
			retCh <- session
			count++
		}
		for (maxCount <= 0 || count < maxCount) && resp.NextStartPrimaryKey != nil {
			rangeReq.RangeRowQueryCriteria.StartPrimaryKey = resp.NextStartPrimaryKey
			resp, err = s.clt.GetRange(rangeReq)
			if err != nil {
				// Log error but can't return it since we're in a goroutine
				return
			}
			for _, row := range resp.Rows {
				var session model.Session
				parseSessionFromRow(&session, row.Columns, row.PrimaryKey)
				retCh <- session
				count++
			}
		}
	}()
	return retCh
}

func (s *MemoryStore) ListRecentSessions(userID string, filter tablestore.ColumnFilter, inclusiveStartUpdateTime int64, inclusiveEndUpdateTime int64, maxCount int, batchSize int) ([]model.Session, error) {
	startPk := new(tablestore.PrimaryKey)
	if userID != "" {
		startPk.AddPrimaryKeyColumn(SessionUserIDField, userID)
	} else {
		startPk.AddPrimaryKeyColumnWithMaxValue(SessionUserIDField)
	}
	if inclusiveStartUpdateTime > 0 {
		startPk.AddPrimaryKeyColumn(SessionUpdateTimeField, inclusiveStartUpdateTime)
	} else {
		startPk.AddPrimaryKeyColumnWithMaxValue(SessionUpdateTimeField)
	}
	startPk.AddPrimaryKeyColumnWithMaxValue(SessionSessionIDField)
	endPk := new(tablestore.PrimaryKey)
	if userID != "" {
		endPk.AddPrimaryKeyColumn(SessionUserIDField, userID)
	} else {
		endPk.AddPrimaryKeyColumnWithMinValue(SessionUserIDField)
	}
	if inclusiveEndUpdateTime > 0 {
		endPk.AddPrimaryKeyColumn(SessionUpdateTimeField, inclusiveEndUpdateTime)
	} else {
		endPk.AddPrimaryKeyColumnWithMinValue(SessionUpdateTimeField)
	}
	endPk.AddPrimaryKeyColumnWithMinValue(SessionSessionIDField)
	criteria := new(tablestore.RangeRowQueryCriteria)
	criteria.TableName = s.SessionSecondaryIndexName
	criteria.StartPrimaryKey = startPk
	criteria.EndPrimaryKey = endPk
	criteria.Direction = tablestore.BACKWARD
	criteria.MaxVersion = 1
	if filter != nil {
		criteria.Filter = filter
	}
	if maxCount <= 0 {
		maxCount = -1
	}
	criteria.Limit = int32(configBatchSize(batchSize, maxCount, filter))
	rangeReq := new(tablestore.GetRangeRequest)
	rangeReq.RangeRowQueryCriteria = criteria
	resp, err := s.clt.GetRange(rangeReq)
	if err != nil {
		return nil, fmt.Errorf("failed to get list of sessions, %w", err)
	}
	hitsLimit := 5000
	if maxCount > 0 {
		hitsLimit = maxCount
	}
	hits := make([]model.Session, 0, hitsLimit)
	for _, row := range resp.Rows {
		var session model.Session
		parseSessionFromRow(&session, row.Columns, row.PrimaryKey)
		hits = append(hits, session)
	}
	for (maxCount <= 0 || len(hits) < maxCount) && resp.NextStartPrimaryKey != nil {
		rangeReq.RangeRowQueryCriteria.StartPrimaryKey = resp.NextStartPrimaryKey
		resp, err = s.clt.GetRange(rangeReq)
		if err != nil {
			return nil, fmt.Errorf("failed to get list of sessions, %w", err)
		}
		for _, row := range resp.Rows {
			var session model.Session
			parseSessionFromRow(&session, row.Columns, row.PrimaryKey)
			hits = append(hits, session)
		}
	}
	return hits, nil
}

func (s *MemoryStore) ListRecentSessionsPaginated(userID string, filter tablestore.ColumnFilter, inclusiveStartUpdateTime int64, inclusiveEndUpdateTime int64, pageSize int, nextStartPrimaryKey *tablestore.PrimaryKey) (*model.Response[model.Session], error) {
	var startPk *tablestore.PrimaryKey
	if nextStartPrimaryKey != nil {
		startPk = nextStartPrimaryKey
	} else {
		startPk = new(tablestore.PrimaryKey)
		if userID != "" {
			startPk.AddPrimaryKeyColumn(SessionUserIDField, userID)
		} else {
			startPk.AddPrimaryKeyColumnWithMaxValue(SessionUserIDField)
		}
		if inclusiveStartUpdateTime > 0 {
			startPk.AddPrimaryKeyColumn(SessionUpdateTimeField, inclusiveStartUpdateTime)
		} else {
			startPk.AddPrimaryKeyColumnWithMaxValue(SessionUpdateTimeField)
		}
		startPk.AddPrimaryKeyColumnWithMaxValue(SessionSessionIDField)
	}
	endPk := new(tablestore.PrimaryKey)
	if userID != "" {
		endPk.AddPrimaryKeyColumn(SessionUserIDField, userID)
	} else {
		endPk.AddPrimaryKeyColumnWithMinValue(SessionUserIDField)
	}
	if inclusiveEndUpdateTime > 0 {
		endPk.AddPrimaryKeyColumn(SessionUpdateTimeField, inclusiveEndUpdateTime)
	} else {
		endPk.AddPrimaryKeyColumnWithMinValue(SessionUpdateTimeField)
	}
	endPk.AddPrimaryKeyColumnWithMinValue(SessionSessionIDField)
	criteria := new(tablestore.RangeRowQueryCriteria)
	criteria.TableName = s.SessionSecondaryIndexName
	criteria.StartPrimaryKey = startPk
	criteria.EndPrimaryKey = endPk
	criteria.Direction = tablestore.BACKWARD
	criteria.MaxVersion = 1
	if filter != nil {
		criteria.Filter = filter
	}
	criteria.Limit = int32(pageSize)
	rangeReq := new(tablestore.GetRangeRequest)
	rangeReq.RangeRowQueryCriteria = criteria
	resp, err := s.clt.GetRange(rangeReq)
	if err != nil {
		return nil, fmt.Errorf("failed to get list of sessions, %w", err)
	}
	ret := new(model.Response[model.Session])
	ret.Hits = make([]model.Session, 0, len(resp.Rows))
	for _, row := range resp.Rows {
		var session model.Session
		parseSessionFromRow(&session, row.Columns, row.PrimaryKey)
		ret.Hits = append(ret.Hits, session)
	}
	if resp.NextStartPrimaryKey != nil {
		ret.NextStartPrimaryKey = resp.NextStartPrimaryKey
	}
	return ret, nil
}

func (s *MemoryStore) SearchSessions(userID string, keyword string, inclusiveStartUpdateTime int64, inclusiveEndUpdateTime int64, pageSize int32, nextToken []byte) (*model.Response[model.Session], error) {
	searchReq := new(tablestore.SearchRequest)
	searchReq.SetTableName(s.SessionTableName)
	searchReq.SetIndexName(s.SessionSearchIndexName)
	queries := make([]search.Query, 0, 3)
	if userID != "" {
		queries = append(queries, &search.TermQuery{
			FieldName: SessionUserIDField,
			Term:      userID,
		})
	}
	if inclusiveStartUpdateTime > 0 || inclusiveEndUpdateTime > 0 {
		rangeQuery := &search.RangeQuery{
			FieldName: SessionUpdateTimeField,
		}
		if inclusiveStartUpdateTime > 0 {
			rangeQuery.From = inclusiveStartUpdateTime
			rangeQuery.IncludeLower = true
		} else {
			rangeQuery.From = tablestore.MIN
		}
		if inclusiveEndUpdateTime > 0 {
			rangeQuery.To = inclusiveEndUpdateTime
			rangeQuery.IncludeUpper = true
		} else {
			rangeQuery.To = tablestore.MAX
		}
		queries = append(queries, rangeQuery)
	}
	if keyword != "" {
		queries = append(queries, &search.MatchPhraseQuery{
			FieldName: SessionSearchContentField,
			Text:      keyword,
		})
	}
	searchQuery := search.NewSearchQuery()
	if l := len(queries); l > 1 {
		searchQuery.SetQuery(&search.BoolQuery{
			MustQueries: queries,
		})
	} else if l == 1 {
		searchQuery.SetQuery(queries[0])
	} else {
		return nil, errors.New("missing search conditions")
	}
	searchQuery.SetSort(&search.Sort{
		Sorters: []search.Sorter{
			&search.ScoreSort{
				Order: search.SortOrder_DESC.Enum(), // 从得分高到低排序。
			},
		},
	})
	searchQuery.SetGetTotalCount(true)
	searchQuery.SetLimit(pageSize)
	if nextToken != nil {
		searchQuery.SetToken(nextToken)
	}
	searchReq.SetSearchQuery(searchQuery)
	searchReq.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	resp, err := s.clt.Search(searchReq)
	if err != nil {
		return nil, fmt.Errorf("failed to search sessions, %w", err)
	}
	ret := new(model.Response[model.Session])
	ret.Total = resp.TotalCount
	ret.Hits = make([]model.Session, 0, len(resp.Rows))
	for _, row := range resp.Rows {
		var session model.Session
		parseSessionFromRow(&session, row.Columns, row.PrimaryKey)
		ret.Hits = append(ret.Hits, session)
	}
	if resp.NextToken != nil {
		ret.NextToken = resp.NextToken
	}
	return ret, nil
}
