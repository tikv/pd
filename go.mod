module github.com/tikv/pd

go 1.23

// When you modify PD cooperatively with kvproto, this will be useful to submit the PR to PD and the PR to
// kvproto at the same time. You can run `go mod tidy` to make it replaced with go-mod style specification.
// After the PR to kvproto is merged, remember to comment this out and run `go mod tidy`.
// replace github.com/pingcap/kvproto => github.com/$YourPrivateRepo $YourPrivateBranch

require (
	github.com/AlekSi/gocov-xml v1.0.0
	github.com/BurntSushi/toml v0.3.1
	github.com/aws/aws-sdk-go-v2/config v1.25.12
	github.com/aws/aws-sdk-go-v2/credentials v1.16.10
	github.com/aws/aws-sdk-go-v2/service/kms v1.26.5
	github.com/aws/aws-sdk-go-v2/service/sts v1.26.3
	github.com/axw/gocov v1.0.0
	github.com/brianvoe/gofakeit/v6 v6.26.3
	github.com/cakturk/go-netstat v0.0.0-20200220111822-e5b49efee7a5
	github.com/coreos/go-semver v0.3.1
	github.com/docker/go-units v0.4.0
	github.com/elliotchance/pie/v2 v2.1.0
	github.com/gin-contrib/cors v1.6.0
	github.com/gin-contrib/gzip v0.0.1
	github.com/gin-contrib/pprof v1.4.0
	github.com/gin-gonic/gin v1.10.0
	github.com/gogo/protobuf v1.3.2
	github.com/google/btree v1.1.2
	github.com/gorilla/mux v1.7.4
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/joho/godotenv v1.4.0
	github.com/mailru/easyjson v0.7.6
	github.com/phf/go-queue v0.0.0-20170504031614-9abe38d0371d
	github.com/pingcap/errcode v0.3.0
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/failpoint v0.0.0-20240528011301-b51a646c7c86
	github.com/pingcap/kvproto v0.0.0-20250117122752-2b87602a94a1
	github.com/pingcap/log v1.1.1-0.20221110025148-ca232912c9f3
	github.com/pingcap/sysutil v1.0.1-0.20230407040306-fb007c5aff21
	github.com/pingcap/tidb-dashboard v0.0.0-20241104061623-bce95733dad7
	github.com/prometheus/client_golang v1.20.5
	github.com/prometheus/common v0.55.0
	github.com/sasha-s/go-deadlock v0.3.5
	github.com/shirou/gopsutil/v3 v3.23.3
	github.com/smallnest/chanx v1.2.1-0.20240521153536-01121e21ff99
	github.com/soheilhy/cmux v0.1.5
	github.com/spf13/cobra v1.8.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.9.0
	github.com/swaggo/http-swagger v1.2.6
	github.com/swaggo/swag v1.8.3
	github.com/syndtr/goleveldb v1.0.1-0.20190318030020-c3a204f8e965
	github.com/unrolled/render v1.0.1
	github.com/urfave/negroni v0.3.0
	go.etcd.io/etcd/api/v3 v3.5.15
	go.etcd.io/etcd/client/pkg/v3 v3.5.15
	go.etcd.io/etcd/client/v3 v3.5.15
	go.etcd.io/etcd/server/v3 v3.5.15
	go.uber.org/goleak v1.3.0
	go.uber.org/zap v1.27.0
	golang.org/x/exp v0.0.0-20230711005742-c3f37128e5a4
	golang.org/x/time v0.5.0
	golang.org/x/tools v0.21.1-0.20240508182429-e35e4ccd0d2d
	google.golang.org/grpc v1.62.1
	gotest.tools/gotestsum v1.7.0
)

require (
	github.com/KyleBanks/depth v1.2.1 // indirect
	github.com/Masterminds/semver v1.5.0 // indirect
	github.com/PuerkitoBio/purell v1.1.1 // indirect
	github.com/PuerkitoBio/urlesc v0.0.0-20170810143723-de5bf2ad4578 // indirect
	github.com/ReneKroon/ttlcache/v2 v2.3.0 // indirect
	github.com/VividCortex/mysqlerr v1.0.0 // indirect
	github.com/Xeoncross/go-aesctr-with-hmac v0.0.0-20200623134604-12b17a7ff502 // indirect
	github.com/aws/aws-sdk-go-v2 v1.23.5 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.14.9 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.2.8 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.5.8 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.7.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.10.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.10.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.18.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.21.3 // indirect
	github.com/aws/smithy-go v1.18.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bitly/go-simplejson v0.5.0 // indirect
	github.com/breeswish/gin-jwt/v2 v2.6.4-jwt-patch // indirect
	github.com/bytedance/sonic v1.11.6 // indirect
	github.com/bytedance/sonic/loader v0.1.1 // indirect
	github.com/cenkalti/backoff/v4 v4.2.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cloudwego/base64x v0.1.4 // indirect
	github.com/cloudwego/iasm v0.2.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.3 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dnephin/pflag v1.0.7 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/fatih/color v1.10.0 // indirect
	github.com/fogleman/gg v1.3.0 // indirect
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/gabriel-vasile/mimetype v1.4.3 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/gin-contrib/sse v0.1.0 // indirect
	github.com/go-logr/logr v1.3.0 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-openapi/jsonpointer v0.19.5 // indirect
	github.com/go-openapi/jsonreference v0.19.6 // indirect
	github.com/go-openapi/spec v0.20.4 // indirect
	github.com/go-openapi/swag v0.19.15 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/go-playground/validator/v10 v10.20.0 // indirect
	github.com/go-resty/resty/v2 v2.6.0 // indirect
	github.com/go-sql-driver/mysql v1.7.0 // indirect
	github.com/goccy/go-graphviz v0.1.3 // indirect
	github.com/goccy/go-json v0.10.2 // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang-jwt/jwt/v4 v4.5.1 // indirect
	github.com/golang/freetype v0.0.0-20170609003504-e2365dfdc4a0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/pprof v0.0.0-20211122183932-1daafda22083 // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gorilla/websocket v1.5.1 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.16.0 // indirect
	github.com/gtank/cryptopasta v0.0.0-20170601214702-1f550f6f2f69 // indirect
	github.com/ianlancetaylor/demangle v0.0.0-20210905161508-09a460cdf81d // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/jonboulle/clockwork v0.4.0 // indirect
	github.com/joomcode/errorx v1.0.1 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/klauspost/cpuid/v2 v2.2.7 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20230326075908-cb1d2100619a // indirect
	github.com/mattn/go-colorable v0.1.8 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-sqlite3 v1.14.15 // indirect
	github.com/minio/sio v0.3.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/oleiade/reflections v1.0.1 // indirect
	github.com/onsi/gomega v1.20.1 // indirect
	github.com/pelletier/go-toml/v2 v2.2.2 // indirect
	github.com/petermattis/goid v0.0.0-20240813172612-4fcff4a6cae7 // indirect
	github.com/pingcap/tipb v0.0.0-20220718022156-3e2483c20a9e // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20221212215047-62379fc7944b // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rs/cors v1.7.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/samber/lo v1.37.0 // indirect
	github.com/sergi/go-diff v1.1.0 // indirect
	github.com/shoenig/go-m1cpu v0.1.5 // indirect
	github.com/shurcooL/httpgzip v0.0.0-20190720172056-320755c1c1b0 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/swaggo/files v0.0.0-20210815190702-a29dd2bc99b2 // indirect
	github.com/tidwall/gjson v1.9.3 // indirect
	github.com/tklauser/go-sysconf v0.3.11 // indirect
	github.com/tklauser/numcpus v0.6.0 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20220101234140-673ab2c3ae75 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/ugorji/go/codec v1.2.12 // indirect
	github.com/urfave/cli/v2 v2.3.0 // indirect
	github.com/vmihailenco/msgpack/v5 v5.3.5 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/xiang90/probing v0.0.0-20221125231312-a49e3df8f510 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.etcd.io/bbolt v1.3.10 // indirect
	go.etcd.io/etcd/client/v2 v2.305.15 // indirect
	go.etcd.io/etcd/pkg/v3 v3.5.15 // indirect
	go.etcd.io/etcd/raft/v3 v3.5.15 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.46.0 // indirect
	go.opentelemetry.io/otel v1.20.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.20.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.20.0 // indirect
	go.opentelemetry.io/otel/metric v1.20.0 // indirect
	go.opentelemetry.io/otel/sdk v1.20.0 // indirect
	go.opentelemetry.io/otel/trace v1.20.0 // indirect
	go.opentelemetry.io/proto/otlp v1.0.0 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/dig v1.9.0 // indirect
	go.uber.org/fx v1.12.0 // indirect
	go.uber.org/multierr v1.11.0
	golang.org/x/arch v0.8.0 // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/image v0.18.0 // indirect
	golang.org/x/mod v0.17.0 // indirect
	golang.org/x/net v0.33.0 // indirect
	golang.org/x/oauth2 v0.21.0 // indirect
	golang.org/x/sync v0.10.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/term v0.27.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto v0.0.0-20240401170217-c3f982113cda // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240401170217-c3f982113cda // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240401170217-c3f982113cda // indirect
	google.golang.org/protobuf v1.34.2 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	gorm.io/datatypes v1.1.0 // indirect
	gorm.io/driver/mysql v1.4.5 // indirect
	gorm.io/driver/sqlite v1.4.3 // indirect
	gorm.io/gorm v1.24.3 // indirect
	moul.io/zapgorm2 v1.1.0 // indirect
	sigs.k8s.io/yaml v1.4.0 // indirect
)
