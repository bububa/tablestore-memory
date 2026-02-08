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

func (s *MemoryStore) InitMessageTable() error {
	listResp, err := s.clt.ListTable()
	if err != nil {
		return fmt.Errorf("list message table failed during init message table, %w", err)
	}
	if slices.Contains(listResp.TableNames, s.MessageTableName) {
		describeReq := new(tablestore.DescribeTableRequest)
		describeReq.TableName = s.MessageTableName
		describeResp, err := s.clt.DescribeTable(describeReq)
		if err != nil {
			return fmt.Errorf("describe message table failed during init message table, %w", err)
		}

		// Check if index already exists before attempting to create
		indexExists := false
		for _, v := range describeResp.IndexMetas {
			if v.IndexName == s.MessageSecondaryIndexName {
				indexExists = true
				break
			}
		}

		if !indexExists {
			indexMeta := new(tablestore.IndexMeta)
			indexMeta.IndexName = s.MessageSecondaryIndexName
			indexMeta.AddPrimaryKeyColumn(MessageSessionIDField)
			indexMeta.AddPrimaryKeyColumn(MessageMessageIDField)
			indexMeta.AddPrimaryKeyColumn(MessageCreateTimeField)
			indexMeta.SetAsLocalIndex()
			createIndexReq := new(tablestore.CreateIndexRequest)
			createIndexReq.MainTableName = s.MessageTableName
			createIndexReq.IndexMeta = indexMeta
			if _, err := s.clt.CreateIndex(createIndexReq); err != nil {
				return fmt.Errorf("create message table secondary index failed during init message table, %w", err)
			}
		}
		searchIndexExists := false
		listSearchIndexReq := new(tablestore.ListSearchIndexRequest)
		listSearchIndexReq.TableName = s.MessageTableName
		indexResp, err := s.clt.ListSearchIndex(listSearchIndexReq)
		if err != nil {
			return fmt.Errorf("list message search index failed during init message table, %w", err)
		}
		for _, v := range indexResp.IndexInfo {
			if v.IndexName == s.MessageSearchIndexName {
				searchIndexExists = true
				break
			}
		}

		if !searchIndexExists {
			if err := s.createMessageSearchIndex(); err != nil {
				return fmt.Errorf("create message table search index failed during init message table, %w", err)
			}
		}
		return nil
	}
	tableMeta := new(tablestore.TableMeta)
	tableMeta.TableName = s.MessageTableName
	tableMeta.AddPrimaryKeyColumn(MessageSessionIDField, tablestore.PrimaryKeyType_STRING)
	tableMeta.AddPrimaryKeyColumn(MessageCreateTimeField, tablestore.PrimaryKeyType_INTEGER)
	tableMeta.AddPrimaryKeyColumn(MessageMessageIDField, tablestore.PrimaryKeyType_STRING)
	tableMeta.AddDefinedColumn(MessageContentField, tablestore.DefinedColumn_STRING)
	tableOption := new(tablestore.TableOption)
	tableOption.MaxVersion = 1
	tableOption.TimeToAlive = -1
	reservedThroughput := new(tablestore.ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0
	indexMeta := new(tablestore.IndexMeta)
	indexMeta.IndexName = s.MessageSecondaryIndexName
	indexMeta.AddPrimaryKeyColumn(MessageSessionIDField)
	indexMeta.AddPrimaryKeyColumn(MessageMessageIDField)
	indexMeta.AddPrimaryKeyColumn(MessageCreateTimeField)
	indexMeta.SetAsLocalIndex()
	createTableRequest := new(tablestore.CreateTableRequest)
	createTableRequest.TableMeta = tableMeta
	createTableRequest.TableOption = tableOption
	createTableRequest.ReservedThroughput = reservedThroughput
	createTableRequest.AddIndexMeta(indexMeta)
	if _, err := s.clt.CreateTable(createTableRequest); err != nil {
		return fmt.Errorf("create message table failed, %w", err)
	}
	return nil
}

func (s *MemoryStore) createMessageSearchIndex() error {
	analyzer := tablestore.Analyzer_Fuzzy
	createReq := new(tablestore.CreateSearchIndexRequest)
	createReq.TableName = s.MessageTableName
	createReq.IndexName = s.MessageSearchIndexName
	createReq.IndexSchema = &tablestore.IndexSchema{
		FieldSchemas: []*tablestore.FieldSchema{
			{
				FieldName: proto.String(MessageSessionIDField),
				FieldType: tablestore.FieldType_KEYWORD,
				Index:     proto.Bool(true),
			},
			{
				FieldName: proto.String(MessageCreateTimeField),
				FieldType: tablestore.FieldType_LONG,
				Index:     proto.Bool(true),
			},
			{
				FieldName: proto.String(MessageSearchContentField),
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
		return fmt.Errorf("create message search index failed, %w", err)
	}
	return nil
}

func (s *MemoryStore) PutMessage(message *model.Message) error {
	pk := new(tablestore.PrimaryKey)
	pk.AddPrimaryKeyColumn(MessageSessionIDField, message.SessionID)
	pk.AddPrimaryKeyColumn(MessageCreateTimeField, message.CreateTime)
	pk.AddPrimaryKeyColumn(MessageMessageIDField, message.MessageID)
	putReq := new(tablestore.PutRowRequest)
	putReq.PutRowChange = new(tablestore.PutRowChange)
	putReq.PutRowChange.TableName = s.MessageTableName
	putReq.PutRowChange.PrimaryKey = pk
	if message.Content != "" {
		putReq.PutRowChange.AddColumn(MessageContentField, message.Content)
	}
	if message.SearchContent != "" {
		putReq.PutRowChange.AddColumn(MessageSearchContentField, message.SearchContent)
	}
	for k, v := range message.Metadata {
		putReq.PutRowChange.AddColumn(k, v)
	}
	putReq.PutRowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
	if _, err := s.clt.PutRow(putReq); err != nil {
		return fmt.Errorf("put message to memory store failed, %w", err)
	}
	return nil
}

func (s *MemoryStore) UpdateMessage(message *model.Message) error {
	tmp := model.Message{
		SessionID:  message.SessionID,
		MessageID:  message.MessageID,
		CreateTime: message.CreateTime,
	}
	if err := s.GetMessage(&tmp); err != nil {
		return fmt.Errorf("update message failed, %w", err)
	}
	if message.CreateTime == 0 {
		// CreateTime is already populated by the GetMessage call above
		message.CreateTime = tmp.CreateTime
	}
	pk := new(tablestore.PrimaryKey)
	pk.AddPrimaryKeyColumn(MessageSessionIDField, message.SessionID)
	pk.AddPrimaryKeyColumn(MessageCreateTimeField, message.CreateTime)
	pk.AddPrimaryKeyColumn(MessageMessageIDField, message.MessageID)
	updateReq := new(tablestore.UpdateRowRequest)
	updateReq.UpdateRowChange = new(tablestore.UpdateRowChange)
	updateReq.UpdateRowChange.TableName = s.MessageTableName
	updateReq.UpdateRowChange.PrimaryKey = pk
	if message.Content != "" {
		updateReq.UpdateRowChange.PutColumn(MessageContentField, message.Content)
	} else {
		updateReq.UpdateRowChange.DeleteColumn(MessageContentField)
	}
	if message.SearchContent != "" {
		updateReq.UpdateRowChange.PutColumn(MessageSearchContentField, message.SearchContent)
	} else {
		updateReq.UpdateRowChange.DeleteColumn(MessageSearchContentField)
	}
	for k, v := range message.Metadata {
		updateReq.UpdateRowChange.PutColumn(k, v)
	}
	for k := range tmp.Metadata {
		if _, ok := message.Metadata[k]; !ok {
			updateReq.UpdateRowChange.DeleteColumn(k)
		}
	}
	updateReq.UpdateRowChange.SetCondition(tablestore.RowExistenceExpectation_EXPECT_EXIST)
	if _, err := s.clt.UpdateRow(updateReq); err != nil {
		return fmt.Errorf("update message in memory store failed, %w", err)
	}
	return nil
}

func (s *MemoryStore) DeleteMessage(sessionID string, messageID string, createTime int64) error {
	if createTime == 0 {
		tmp := model.Message{
			SessionID: sessionID,
			MessageID: messageID,
		}
		if err := s.getMessageCreateTimeFromSecondaryIndex(&tmp); err != nil {
			return fmt.Errorf("delete message failed, %w", err)
		}
		createTime = tmp.CreateTime
	}
	pk := new(tablestore.PrimaryKey)
	pk.AddPrimaryKeyColumn(MessageSessionIDField, sessionID)
	pk.AddPrimaryKeyColumn(MessageCreateTimeField, createTime)
	pk.AddPrimaryKeyColumn(MessageMessageIDField, messageID)
	deleteReq := new(tablestore.DeleteRowRequest)
	deleteReq.DeleteRowChange = new(tablestore.DeleteRowChange)
	deleteReq.DeleteRowChange.TableName = s.MessageTableName
	deleteReq.DeleteRowChange.PrimaryKey = pk
	deleteReq.DeleteRowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
	if _, err := s.clt.DeleteRow(deleteReq); err != nil {
		return fmt.Errorf("delete message in memory store failed, %w", err)
	}
	return nil
}

// DeleteMessages delete all messages for a session
func (s *MemoryStore) DeleteMessages(sessionID string) (int, error) {
	list := s.ListMessages(sessionID)
	var (
		count int
		total int
	)
	// Process items in batches of 200
	currentBatch := new(tablestore.BatchWriteRowRequest)
	for v := range list {
		rowChange := new(tablestore.DeleteRowChange)
		rowChange.TableName = s.MessageTableName
		rowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
		pk := new(tablestore.PrimaryKey)
		pk.AddPrimaryKeyColumn(MessageSessionIDField, v.SessionID)
		pk.AddPrimaryKeyColumn(MessageCreateTimeField, v.CreateTime)
		pk.AddPrimaryKeyColumn(MessageMessageIDField, v.MessageID)
		rowChange.PrimaryKey = pk
		currentBatch.AddRowChange(rowChange)
		count++

		if count >= 200 {
			if _, err := s.clt.BatchWriteRow(currentBatch); err != nil {
				return total, fmt.Errorf("delete session messages failed, %w", err)
			}
			total += count
			count = 0
			// Create a new batch request for next batch
			currentBatch = new(tablestore.BatchWriteRowRequest)
		}
	}
	if count > 0 {
		if _, err := s.clt.BatchWriteRow(currentBatch); err != nil {
			return total, fmt.Errorf("delete session messages failed, %w", err)
		}
		total += count
	}
	return total, nil
}

func (s *MemoryStore) DeleteAllMessages() (int, error) {
	list := s.ListAllMessages()
	var (
		count int
		total int
	)
	// Process items in batches of 200
	currentBatch := new(tablestore.BatchWriteRowRequest)
	for v := range list {
		rowChange := new(tablestore.DeleteRowChange)
		rowChange.TableName = s.MessageTableName
		rowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
		pk := new(tablestore.PrimaryKey)
		pk.AddPrimaryKeyColumn(MessageSessionIDField, v.SessionID)
		pk.AddPrimaryKeyColumn(MessageCreateTimeField, v.CreateTime)
		pk.AddPrimaryKeyColumn(MessageMessageIDField, v.MessageID)
		rowChange.PrimaryKey = pk
		currentBatch.AddRowChange(rowChange)
		count++

		if count >= 200 {
			if _, err := s.clt.BatchWriteRow(currentBatch); err != nil {
				return total, fmt.Errorf("delete session messages failed, %w", err)
			}
			total += count
			count = 0
			// Create a new batch request for next batch
			currentBatch = new(tablestore.BatchWriteRowRequest)
		}
	}
	if count > 0 {
		if _, err := s.clt.BatchWriteRow(currentBatch); err != nil {
			return total, fmt.Errorf("delete session messages failed, %w", err)
		}
		total += count
	}
	return total, nil
}

func (s *MemoryStore) GetMessage(message *model.Message) error {
	if message.CreateTime == 0 {
		tmp := model.Message{
			SessionID: message.SessionID,
			MessageID: message.MessageID,
		}
		if err := s.getMessageCreateTimeFromSecondaryIndex(&tmp); err != nil {
			return fmt.Errorf("update message failed, %w", err)
		}
		message.CreateTime = tmp.CreateTime
	}
	pk := new(tablestore.PrimaryKey)
	pk.AddPrimaryKeyColumn(MessageSessionIDField, message.SessionID)
	pk.AddPrimaryKeyColumn(MessageCreateTimeField, message.CreateTime)
	pk.AddPrimaryKeyColumn(MessageMessageIDField, message.MessageID)
	getReq := new(tablestore.GetRowRequest)
	getReq.SingleRowQueryCriteria = new(tablestore.SingleRowQueryCriteria)
	getReq.SingleRowQueryCriteria.TableName = s.MessageTableName
	getReq.SingleRowQueryCriteria.PrimaryKey = pk
	getReq.SingleRowQueryCriteria.MaxVersion = 1
	resp, err := s.clt.GetRow(getReq)
	if err != nil {
		return fmt.Errorf("failed to get message in memory store, %w", err)
	}
	if len(resp.PrimaryKey.PrimaryKeys) == 0 {
		return fmt.Errorf("message not exists")
	}
	parseMessageFromRow(message, resp.Columns, &resp.PrimaryKey)
	return nil
}

// ListAllMessages list all messages
func (s *MemoryStore) ListAllMessages() <-chan model.Message {
	return s.ListMessagesWithFilter("", nil, 0, 0, tablestore.FORWARD, -1, 5000)
}

// ListMessages list messages for a session
func (s *MemoryStore) ListMessages(sessionID string) <-chan model.Message {
	return s.ListMessagesWithFilter(sessionID, nil, 0, 0, tablestore.FORWARD, -1, 5000)
}

// ListMessagesWithFilter  list messages with filters
func (s *MemoryStore) ListMessagesWithFilter(
	sessionID string,
	filter tablestore.ColumnFilter,
	inclusiveStartCreateTime int64,
	inclusiveEndCreateTime int64,
	order tablestore.Direction,
	maxCount int,
	batchSize int,
) <-chan model.Message {
	var (
		constMin = tablestore.MIN
		constMax = tablestore.MAX
	)
	if order == tablestore.BACKWARD {
		constMin = tablestore.MAX
		constMax = tablestore.MIN
	}

	startPk := new(tablestore.PrimaryKey)
	if sessionID != "" {
		startPk.AddPrimaryKeyColumn(MessageSessionIDField, sessionID)
	} else {
		startPk.PrimaryKeys = append(startPk.PrimaryKeys, &tablestore.PrimaryKeyColumn{ColumnName: MessageSessionIDField, PrimaryKeyOption: constMin})
	}
	if inclusiveStartCreateTime > 0 {
		startPk.AddPrimaryKeyColumn(MessageCreateTimeField, inclusiveStartCreateTime)
	} else {
		startPk.PrimaryKeys = append(startPk.PrimaryKeys, &tablestore.PrimaryKeyColumn{ColumnName: MessageCreateTimeField, PrimaryKeyOption: constMin})
	}
	startPk.PrimaryKeys = append(startPk.PrimaryKeys, &tablestore.PrimaryKeyColumn{ColumnName: MessageMessageIDField, PrimaryKeyOption: constMin})
	endPk := new(tablestore.PrimaryKey)
	if sessionID != "" {
		endPk.AddPrimaryKeyColumn(MessageSessionIDField, sessionID)
	} else {
		endPk.PrimaryKeys = append(endPk.PrimaryKeys, &tablestore.PrimaryKeyColumn{ColumnName: MessageSessionIDField, PrimaryKeyOption: constMax})
	}
	if inclusiveEndCreateTime > 0 {
		endPk.AddPrimaryKeyColumn(MessageCreateTimeField, inclusiveEndCreateTime)
	} else {
		endPk.PrimaryKeys = append(endPk.PrimaryKeys, &tablestore.PrimaryKeyColumn{ColumnName: MessageCreateTimeField, PrimaryKeyOption: constMax})
	}
	endPk.PrimaryKeys = append(endPk.PrimaryKeys, &tablestore.PrimaryKeyColumn{ColumnName: MessageMessageIDField, PrimaryKeyOption: constMax})
	criteria := new(tablestore.RangeRowQueryCriteria)
	criteria.TableName = s.MessageTableName
	criteria.StartPrimaryKey = startPk
	criteria.EndPrimaryKey = endPk
	criteria.Direction = order
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
	retCh := make(chan model.Message)

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
			var msg model.Message
			parseMessageFromRow(&msg, row.Columns, row.PrimaryKey)
			retCh <- msg
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
				var msg model.Message
				parseMessageFromRow(&msg, row.Columns, row.PrimaryKey)
				retCh <- msg
				count++
			}
		}
	}()
	return retCh
}

// ListMessagesPaginated  paginated messages
func (s *MemoryStore) ListMessagesPaginated(
	sessionID string,
	filter tablestore.ColumnFilter,
	inclusiveStartCreateTime int64,
	inclusiveEndCreateTime int64,
	order tablestore.Direction,
	pageSize int,
	nextStartPrimaryKey *tablestore.PrimaryKey,
) (*model.Response[model.Message], error) {
	var (
		constMin = tablestore.MIN
		constMax = tablestore.MAX
	)
	if order == tablestore.BACKWARD {
		constMin = tablestore.MAX
		constMax = tablestore.MIN
	}
	var startPk *tablestore.PrimaryKey
	if nextStartPrimaryKey != nil {
		startPk = nextStartPrimaryKey
	} else {
		startPk = new(tablestore.PrimaryKey)
		if sessionID != "" {
			startPk.AddPrimaryKeyColumn(MessageSessionIDField, sessionID)
		} else {
			startPk.PrimaryKeys = append(startPk.PrimaryKeys, &tablestore.PrimaryKeyColumn{ColumnName: MessageSessionIDField, PrimaryKeyOption: constMin})
		}
		if inclusiveStartCreateTime > 0 {
			startPk.AddPrimaryKeyColumn(MessageCreateTimeField, inclusiveStartCreateTime)
		} else {
			startPk.PrimaryKeys = append(startPk.PrimaryKeys, &tablestore.PrimaryKeyColumn{ColumnName: MessageCreateTimeField, Value: constMin})
		}
		startPk.PrimaryKeys = append(startPk.PrimaryKeys, &tablestore.PrimaryKeyColumn{ColumnName: MessageMessageIDField, Value: constMin})
	}
	endPk := new(tablestore.PrimaryKey)
	if sessionID != "" {
		endPk.AddPrimaryKeyColumn(MessageSessionIDField, sessionID)
	} else {
		endPk.PrimaryKeys = append(endPk.PrimaryKeys, &tablestore.PrimaryKeyColumn{ColumnName: MessageSessionIDField, Value: constMax})
	}
	if inclusiveEndCreateTime > 0 {
		endPk.AddPrimaryKeyColumn(MessageCreateTimeField, inclusiveEndCreateTime)
	} else {
		endPk.PrimaryKeys = append(endPk.PrimaryKeys, &tablestore.PrimaryKeyColumn{ColumnName: MessageCreateTimeField, Value: constMax})
	}
	endPk.PrimaryKeys = append(endPk.PrimaryKeys, &tablestore.PrimaryKeyColumn{ColumnName: MessageMessageIDField, Value: constMax})
	criteria := new(tablestore.RangeRowQueryCriteria)
	criteria.TableName = s.MessageTableName
	criteria.StartPrimaryKey = startPk
	criteria.EndPrimaryKey = endPk
	criteria.Direction = order
	criteria.MaxVersion = 1
	if filter != nil {
		criteria.Filter = filter
	}
	criteria.Limit = int32(pageSize)
	rangeReq := new(tablestore.GetRangeRequest)
	rangeReq.RangeRowQueryCriteria = criteria
	resp, err := s.clt.GetRange(rangeReq)
	if err != nil {
		return nil, fmt.Errorf("failed to get list of messages, %w", err)
	}
	ret := new(model.Response[model.Message])
	ret.Hits = make([]model.Message, 0, len(resp.Rows))
	for _, row := range resp.Rows {
		var message model.Message
		parseMessageFromRow(&message, row.Columns, row.PrimaryKey)
		ret.Hits = append(ret.Hits, message)
	}
	if resp.NextStartPrimaryKey != nil {
		ret.NextStartPrimaryKey = resp.NextStartPrimaryKey
	}
	return ret, nil
}

func (s *MemoryStore) getMessageCreateTimeFromSecondaryIndex(message *model.Message) error {
	startPk := new(tablestore.PrimaryKey)
	// For secondary index, the primary key order is different: SessionID, MessageID, CreateTime
	startPk.AddPrimaryKeyColumn(MessageSessionIDField, message.SessionID)
	startPk.AddPrimaryKeyColumn(MessageMessageIDField, message.MessageID)
	startPk.AddPrimaryKeyColumnWithMinValue(MessageCreateTimeField)
	endPk := new(tablestore.PrimaryKey)
	endPk.AddPrimaryKeyColumn(MessageSessionIDField, message.SessionID)
	endPk.AddPrimaryKeyColumn(MessageMessageIDField, message.MessageID)
	endPk.AddPrimaryKeyColumnWithMaxValue(MessageCreateTimeField)
	criteria := new(tablestore.RangeRowQueryCriteria)
	criteria.TableName = s.MessageSecondaryIndexName
	criteria.StartPrimaryKey = startPk
	criteria.EndPrimaryKey = endPk
	criteria.Direction = tablestore.FORWARD
	criteria.MaxVersion = 1
	criteria.Limit = 1
	rangeReq := new(tablestore.GetRangeRequest)
	rangeReq.RangeRowQueryCriteria = criteria
	resp, err := s.clt.GetRange(rangeReq)
	if err != nil {
		return fmt.Errorf("get message create time from secondary index failed, %w", err)
	}
	if l := len(resp.Rows); l == 0 {
		return fmt.Errorf("message is not exist because createTime is null and can't find in secondaryIndex, sessionId:%s, messageId:%s", message.SessionID, message.MessageID)
	} else if l > 1 {
		return fmt.Errorf("message is not unique, sessionId:%s, messageId:%s, details messages:[%d]", message.SessionID, message.MessageID, l)
	}
	parseMessageFromRow(message, resp.Rows[0].Columns, resp.Rows[0].PrimaryKey)
	return nil
}

func (s *MemoryStore) SearchMessages(sessionID string, keyword string, inclusiveStartCreateTime int64, inclusiveEndCreateTime int64, pageSize int32, nextToken []byte) (*model.Response[model.Message], error) {
	searchReq := new(tablestore.SearchRequest)
	searchReq.SetTableName(s.MessageTableName)
	searchReq.SetIndexName(s.MessageSearchIndexName)
	queries := make([]search.Query, 0, 3)
	if sessionID != "" {
		queries = append(queries, &search.TermQuery{
			FieldName: MessageSessionIDField,
			Term:      sessionID,
		})
	}
	if inclusiveStartCreateTime > 0 || inclusiveEndCreateTime > 0 {
		rangeQuery := &search.RangeQuery{
			FieldName: MessageCreateTimeField,
		}
		if inclusiveStartCreateTime > 0 {
			rangeQuery.From = inclusiveStartCreateTime
			rangeQuery.IncludeLower = true
		} else {
			rangeQuery.From = tablestore.MIN
		}
		if inclusiveEndCreateTime > 0 {
			rangeQuery.To = inclusiveEndCreateTime
			rangeQuery.IncludeUpper = true
		} else {
			rangeQuery.To = tablestore.MAX
		}
		queries = append(queries, rangeQuery)
	}
	if keyword != "" {
		queries = append(queries, &search.MatchPhraseQuery{
			FieldName: MessageSearchContentField,
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
		return nil, fmt.Errorf("failed to search messages, %w", err)
	}
	ret := new(model.Response[model.Message])
	ret.Total = resp.TotalCount
	ret.Hits = make([]model.Message, 0, len(resp.Rows))
	for _, row := range resp.Rows {
		var message model.Message
		parseMessageFromRow(&message, row.Columns, row.PrimaryKey)
		ret.Hits = append(ret.Hits, message)
	}
	if resp.NextToken != nil {
		ret.NextToken = resp.NextToken
	}
	return ret, nil
}
