package cfg

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/DRSN-tech/go-backend/pkg/e"
	"github.com/jimlawless/whereami"
)

type Config struct {
	Minio   *MinIOCfg
	Http    *HTTPConfig
	Grpc    *GRPCConfig
	Db      *PGDBCfg
	Qdrant  *QdrantCfg
	Redis   *RedisCfg
	Ml      *MLServiceCfg
	Kafka   *KafkaCfg
	Metrics *MetricsConfig
}

type KafkaCfg struct {
	Topic             string
	Brokers           []string
	NetworkMode       string
	Partitions        int
	ReplicationFactor int
}

type MinIOCfg struct {
	MinioEndpoint     string // Адрес конечной точки Minio
	BucketName        string // Название конкретного бакета в Minio
	MinioRootUser     string // Имя пользователя для доступа к Minio
	MinioRootPassword string // Пароль для доступа к Minio
	MinioUseSSL       bool   // Переменная, отвечающая за
	UploadImagesLimit int    // Лимит на макс кол-во загружаемых в S3 фото
}

type HTTPConfig struct {
	Port         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

type MetricsConfig struct {
	Port string
}

type GRPCConfig struct {
	Port        string
	NetworkMode string
}

type PGDBCfg struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
	SSLMode  string
}

type QdrantCfg struct {
	Port                 int
	Host                 string
	ApiKey               string
	QdrantCollectionName string // имя коллекции в Qdrant
	UseTLS               bool
	VectorSize           uint64
}

type RedisCfg struct {
	Addr        string
	Password    string
	User        string
	DB          int
	MaxRetries  int
	DialTimeout time.Duration
	Timeout     time.Duration
	ProductTTL  time.Duration
}

type MLServiceCfg struct {
	Addr          string
	MaxConcurrent int
	MaxRetries    int
}

// Load безопасно загружает конфигурацию и возвращает ошибку в случае неудачи.
func Load() (*Config, error) {
	db, err := loadPGDBCfg()
	if err != nil {
		return nil, e.Wrap(whereami.WhereAmI(), err)
	}

	http, err := loadHTTPConfig()
	if err != nil {
		return nil, e.Wrap(whereami.WhereAmI(), err)
	}

	redis, err := loadRedisCfg()
	if err != nil {
		return nil, e.Wrap(whereami.WhereAmI(), err)
	}

	minio, err := loadMinIOCfg()
	if err != nil {
		return nil, e.Wrap(whereami.WhereAmI(), err)
	}

	qdrant, err := loadQdrantCfg()
	if err != nil {
		return nil, e.Wrap(whereami.WhereAmI(), err)
	}

	kafka, err := loadKafkaCfg()
	if err != nil {
		return nil, e.Wrap(whereami.WhereAmI(), err)
	}

	return &Config{
		Minio:   minio,
		Http:    http,
		Grpc:    loadGRPCConfig(),
		Db:      db,
		Qdrant:  qdrant,
		Redis:   redis,
		Ml:      loadMLServiceCfg(),
		Kafka:   kafka,
		Metrics: loadMetricsCfg(),
	}, nil
}

func loadMetricsCfg() *MetricsConfig {
	const (
		defaultPort = "2112"
	)

	port := getEnvOrDefault("METRICS_PORT", defaultPort)

	return &MetricsConfig{Port: port}
}

func loadKafkaCfg() (*KafkaCfg, error) {
	const (
		defaultPartitions        = 3
		defaultReplicationFactor = 1
		defaultNetworkMode       = "tcp"
	)

	brokerStr := os.Getenv("KAFKA_BROKERS")
	if brokerStr == "" {
		return nil, fmt.Errorf("KAFKA_BROKERS environment variable is required")
	}
	brokers := strings.Split(brokerStr, ",")

	topic := os.Getenv("KAFKA_TOPIC")

	if topic == "" {
		return nil, fmt.Errorf("KAFKA_TOPIC environment variable is required")
	}

	partitions, err := parseIntEnv("KAFKA_PARTITIONS", defaultPartitions)
	if err != nil {
		return nil, e.Wrap("KAFKA_PARTITIONS", err)
	}

	replicationFactor, err := parseIntEnv("REPLICATION_FACTOR", defaultReplicationFactor)
	if err != nil {
		return nil, e.Wrap("REPLICATION_FACTOR", err)
	}

	networkMode := getEnvOrDefault("KAFKA_NETWORK_MODE", defaultNetworkMode)

	return &KafkaCfg{
		Brokers:           brokers,
		Topic:             topic,
		Partitions:        partitions,
		ReplicationFactor: replicationFactor,
		NetworkMode:       networkMode,
	}, nil
}

func loadMinIOCfg() (*MinIOCfg, error) {
	const (
		defaultUseSSL   = false
		defaultEndpoint = "minio:9000"
	)

	useSSL, err := strconv.ParseBool(getEnvOrDefault("MINIO_USE_SSL", strconv.FormatBool(defaultUseSSL)))
	if err != nil {
		return nil, err
	}

	return &MinIOCfg{
		MinioEndpoint:     getEnvOrDefault("MINIO_ENDPOINT", defaultEndpoint),
		BucketName:        getEnv("BUCKET_NAME"),
		MinioRootUser:     getEnv("MINIO_ROOT_USER"),
		MinioRootPassword: getEnv("MINIO_ROOT_PASSWORD"),
		MinioUseSSL:       useSSL,
		UploadImagesLimit: 10,
	}, nil
}

func loadHTTPConfig() (*HTTPConfig, error) {
	const (
		defaultPort         = "8080"
		defaultReadTimeout  = 5 * time.Second
		defaultWriteTimeout = 10 * time.Second
		defaultIdleTimeout  = 60 * time.Second
	)

	port := getEnvOrDefault("HTTP_PORT", defaultPort)

	readTimeout, err := parseDurationEnv("HTTP_READ_TIMEOUT", defaultReadTimeout)
	if err != nil {
		return nil, err
	}

	writeTimeout, err := parseDurationEnv("HTTP_WRITE_TIMEOUT", defaultWriteTimeout)
	if err != nil {
		return nil, err
	}

	idleTimeout, err := parseDurationEnv("KEEP_ALIVE", defaultIdleTimeout)
	if err != nil {
		return nil, err
	}

	return &HTTPConfig{
		Port:         port,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
	}, nil
}

func loadGRPCConfig() *GRPCConfig {
	const (
		defaultPort        = "8091"
		defaultNetworkMode = "tcp"
	)

	return &GRPCConfig{
		Port:        getEnvOrDefault("GRPC_PORT", defaultPort),
		NetworkMode: getEnvOrDefault("GRPC_NETWORK_MODE", defaultNetworkMode),
	}
}

func loadPGDBCfg() (*PGDBCfg, error) {
	const (
		defaultHost    = "localhost"
		defaultPort    = "5432"
		defaultSSLMode = "disable"
	)

	user := getEnv("POSTGRES_USER")
	if user == "" {
		err := fmt.Errorf("POSTGRES_USER is required")
		return nil, err
	}

	password := getEnv("POSTGRES_PASSWORD")
	if password == "" {
		err := fmt.Errorf("POSTGRES_PASSWORD is required")
		return nil, err
	}

	dbName := getEnv("POSTGRES_DB")
	if dbName == "" {
		err := fmt.Errorf("POSTGRES_DB is required")
		return nil, err
	}

	return &PGDBCfg{
		Host:     getEnvOrDefault("POSTGRES_HOST", defaultHost),
		Port:     getEnvOrDefault("POSTGRES_PORT", defaultPort),
		User:     user,
		Password: password,
		DBName:   dbName,
		SSLMode:  getEnvOrDefault("SSL_MODE", defaultSSLMode),
	}, nil
}

func loadQdrantCfg() (*QdrantCfg, error) {
	const (
		defaultQdrantGRPCPort = "6334"
		defaultUseTLS         = false
		defaultVectorSize     = "768"
	)

	strPort := getEnvOrDefault("QDRANT_GRPC_PORT", defaultQdrantGRPCPort)
	port, err := strconv.Atoi(strPort)
	if err != nil {
		return nil, err
	}

	useTLS, err := strconv.ParseBool(getEnvOrDefault("QDRANT_USE_TLS", strconv.FormatBool(defaultUseTLS)))
	if err != nil {
		return nil, err
	}

	strVectorSize := getEnvOrDefault("VECTOR_SIZE", defaultVectorSize)
	vectorSize, err := strconv.ParseUint(strVectorSize, 10, 64)
	if err != nil {
		return nil, err
	}

	return &QdrantCfg{
		Host:                 getEnv("QDRANT_HOST"),
		Port:                 port,
		ApiKey:               getEnv("QDRANT__SERVICE__API_KEY"),
		QdrantCollectionName: getEnv("COLLECTION_NAME"),
		UseTLS:               useTLS,
		VectorSize:           vectorSize,
	}, nil
}

func loadRedisCfg() (*RedisCfg, error) {
	const (
		defaultAddr         = "localhost:6379"
		defaultDB           = 0
		defaultMaxRetries   = 3
		defaultDialTimeout  = 5 * time.Second
		defaultReadTimeout  = 3 * time.Second
		defaultWriteTimeout = 3 * time.Second
		defaultProductTTL   = 3 * time.Minute
	)

	addr := getEnvOrDefault("REDIS_ADDR", defaultAddr)
	password := getEnv("REDIS_PASSWORD")
	user := getEnv("REDIS_USER")

	dbStr := getEnvOrDefault("REDIS_DB_ID", strconv.Itoa(defaultDB))
	db, err := strconv.Atoi(dbStr)
	if err != nil {
		return nil, err
	}

	maxRetriesStr := getEnvOrDefault("MAX_RETRIES", strconv.Itoa(defaultMaxRetries))
	maxRetries, err := strconv.Atoi(maxRetriesStr)
	if err != nil {
		return nil, err
	}

	dialTimeout, err := parseDurationEnv("DIAL_TIMEOUT", defaultDialTimeout)
	if err != nil {
		return nil, err
	}

	readTimeout, err := parseDurationEnv("READ_TIMEOUT", defaultReadTimeout)
	if err != nil {
		return nil, err
	}

	writeTimeout, err := parseDurationEnv("WRITE_TIMEOUT", defaultWriteTimeout)
	if err != nil {
		return nil, err
	}

	productTTL, err := parseDurationEnv("PRODUCT_TTL", defaultProductTTL)
	if err != nil {
		return nil, err
	}

	timeout := readTimeout
	if writeTimeout > timeout {
		timeout = writeTimeout
	}

	return &RedisCfg{
		Addr:        addr,
		Password:    password,
		User:        user,
		DB:          db,
		MaxRetries:  maxRetries,
		DialTimeout: dialTimeout,
		Timeout:     timeout,
		ProductTTL:  productTTL,
	}, nil
}

func loadMLServiceCfg() *MLServiceCfg {
	const (
		defaultHost          = "ml-service"
		defaultPort          = "50051"
		defaultMaxConcurrent = 8
		defaultMaxRetries    = 3
	)

	host := getEnvOrDefault("ML_HOST", defaultHost)
	port := getEnvOrDefault("ML_PORT", defaultPort)

	return &MLServiceCfg{
		Addr:          host + ":" + port,
		MaxConcurrent: defaultMaxConcurrent,
		MaxRetries:    defaultMaxRetries,
	}
}

// getEnv возвращает значение переменной окружения.
// Возвращает пустую строку, если переменная не задана.
func getEnv(key string) string {
	return os.Getenv(key)
}

// getEnvOrDefault возвращает значение переменной окружения или значение по умолчанию.
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return defaultValue
}

// parseDurationEnv считывает длительность или возвращает значение по умолчанию.
func parseDurationEnv(key string, defaultValue time.Duration) (time.Duration, error) {
	if v := os.Getenv(key); v != "" {
		return time.ParseDuration(v)
	}

	return defaultValue, nil
}

func parseIntEnv(key string, defaultValue int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue, nil
	}

	intValue, err := strconv.Atoi(v)
	if err != nil {
		return defaultValue, e.ErrIncorrectEnvVariable
	}

	return intValue, nil
}
