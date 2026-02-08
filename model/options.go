package model

type Options struct {
	SessionTableName          string
	MessageTableName          string
	SessionSecondaryIndexName string
	SessionSearchIndexName    string
	MessageSecondaryIndexName string
	MessageSearchIndexName    string
}

type Option func(*Options)

func WithSessionTableName(name string) Option {
	return func(o *Options) {
		o.SessionTableName = name
	}
}

func WithMessageTableName(name string) Option {
	return func(o *Options) {
		o.MessageTableName = name
	}
}

func WithSessionSecondaryIndexName(name string) Option {
	return func(o *Options) {
		o.SessionSecondaryIndexName = name
	}
}

func WithSessionSearchIndexName(name string) Option {
	return func(o *Options) {
		o.SessionSearchIndexName = name
	}
}

func WithMessageSecondaryIndexName(name string) Option {
	return func(o *Options) {
		o.MessageSecondaryIndexName = name
	}
}

func WithMessageSearchIndexName(name string) Option {
	return func(o *Options) {
		o.MessageSearchIndexName = name
	}
}
