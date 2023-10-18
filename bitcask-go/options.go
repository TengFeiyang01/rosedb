package bitcask_go

type Options struct {
	// 数据库数据目录
	DirPath string

	// 数据文件的大小
	DataFileSize int64

	// 每次写入是否持久化
	SyncWrites bool
}
