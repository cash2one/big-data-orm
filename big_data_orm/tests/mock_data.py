big_query_response = {
    "kind": "bigquery#getQueryResultsResponse",
    "etag": 'etag',
    "schema": {
        "fields": [
            {
                "name": 'field_1',
                "type": 'string',
                "mode": 'mode_1',
                "fields": [
                    'field_1'
                ],
                "description": 'string'
            }
        ]
    },
    "jobReference": {
        "projectId": 'string',
        "jobId": 'string'
    },
    "totalRows": 10219231,
    "pageToken": 'TEST123',
    "rows": [
        {
            "f": [
                {
                    "v": 'value_1',
                }
            ]
        }
    ],
    "totalBytesProcessed": long,
    "jobComplete": False,
    "errors": [
        {
            "reason": 'string',
            "location": 'string',
            "debugInfo": 'string',
            "message": 'string'
        }
    ],
    "cacheHit": False,
    "numDmlAffectedRows": long
}
