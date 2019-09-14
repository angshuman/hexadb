# HexaDb
## A schemaless graph database based on RocksDb

HexaDb is a triple based graph data store created on RocksDb storage. It can be used to store, retrieve and query JSON documents.
HexaDb does not require a schema. It also supports relational query through providing nested path or nesting level
in any arbitrary JSON document.

## Naming

HexaDb breaks JSON documents to RDF triples in the format os (S,P,O) and creates six indices. All triples are queryable by those six indices
S,
P,
O,
SP,
PO,
OS

Thus *HexaDb*.

## Building and running locally

`$ docker-compose up`

### Creating your first document

`POST /api/store/app01`

```json
[
    {
        "id": "sensor:0",
        "type": "sensor",
        "name": "Entity sensor 0",
        "temperature": 64.44,
        "humidity": 14.65,
        "pressure": 957.1,
        "marker": {
            "status": "running",
            "red": 9.12,
            "blue": 4.53,
            "green": 9.85
        }
    },
    {
        "id": "sensor:1",
        "type": "sensor",
        "name": "Entity sensor 1",
        "temperature": 65.86,
        "humidity": 12.29,
        "pressure": 945.19,
        "marker": {
            "status": "stopped",
            "red": 9.95,
            "blue": 7.16,
            "green": 2.02
        }
    }
]
```

### Get a document by id

`GET /api/store/app01/sensor:1`

### Find a document by a top level property comparison

`POST /api/store/app01/query`

```json
{
    "filter": {
        "type": {
            "op": "eq",
            "value": "sensor"
        },
        "temperature" : {
            "op" : "gt",
            "value" : 65,
        }
    }
}
```

### Find a document by relationship query

`POST /api/store/app01/query`

```json
{
    "filter": {
        "type": {
            "op": "eq",
            "value": "sensor"
        },
        "outgoing": [
            {
                "path": "marker",
                "target": {
                    "filter": {
                        "green": {
                            "op": "gt",
                            "value": 9
                        }
                    }
                }
            }
        ]
    }
}
```

### Find a document by nesting level

`POST /api/store/app01/query`

```json
{
    "filter": {
        "type": {
            "op": "eq",
            "value": "sensor"
        },
        "outgoing": [
            {
                "path": "*",
                "level" : 2,
                "target": {
                    "filter": {
                        "green": {
                            "op": "gt",
                            "value": 9
                        }
                    }
                }
            }
        ]
    }
}
```

### Update a document

`PATCH /api/store/app01/json`

```json
{
    "id": "sensor:0",
    "name": "Another name",
    "marker": {
        "status": null,
        "red": 1.0
    }
}
```



