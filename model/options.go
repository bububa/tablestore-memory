package model

type Options struct {
	SessionTableName          string
	MessageTableName          string
	SessionSecondaryIndexName string
	MessageSecondaryIndexName string
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

func WithMessageSecondaryIndexName(name string) Option {
	return func(o *Options) {
		o.MessageSecondaryIndexName = name
	}
}
