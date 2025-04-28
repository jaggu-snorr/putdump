# putdump
Put your ndjson data dump back in Elasticsearch.

Best friends with [Blaze](https://github.com/unidentifieddeveloper/blaze) (the fastest way to export your data).

Written in Go.

## Installation

```bash
go build
go install # to install to your $GOPATH/bin directory
```

## Usage

```bash
putdump --dump-file=dump.ndjson --index=index_1 --url=http://localhost:9200
```

Using docker:

```bash
docker build -t putdump .
docker run --rm --net=host -v $(realpath dump.ndjson):/dump.ndjson putdump --dump-file=dump.ndjson --index=index_name --url=http://localhost:27920
```

Note that the index has to exist before you run the putdump command. You can dump all the index information (settings and mappings) with Blaze using `--dump-index-info` and then create the index with your favorite tool. Here is an example using Blaze and curl:

```bash
blaze --host=http://localhost:9200 --index=index_1 --dump-index-info > index_1.json
# you have to remove a few lines from the index info file before your can create an index with it (uuid, creation_date, version, etc.)
curl -XPUT http://localhost:9200/index_2 -H "Content-Type: application/json" -d @index_1.json
```
