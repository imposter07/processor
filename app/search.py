from flask import current_app


def add_to_index(index, model):
    if not current_app.elasticsearch:
        return
    payload = {}
    for field in model.__searchable__:
        payload[field] = getattr(model, field)
    check_index(index)
    current_app.elasticsearch.index(index=index, id=model.id, body=payload)


def remove_from_index(index, model):
    if not current_app.elasticsearch:
        return
    check_index(index)
    current_app.elasticsearch.delete(index=index, id=model.id)


def query_index(index, query, page, per_page):
    if not current_app.elasticsearch:
        return [], 0
    check_index(index)
    search = current_app.elasticsearch.search(
        index=index,
        body={'query': {'multi_match': {'query': query, 'fields': ['*']}},
              'from': (page - 1) * per_page, 'size': per_page})
    ids = [int(hit['_id']) for hit in search['hits']['hits']]
    return ids, len(search['hits']['hits'])


def check_index(index):
    es_settings = current_app.elasticsearch.indices.get_settings()
    blocks_settings = es_settings[index]['settings']['index']['blocks']
    for setting in ['read_only', 'write', 'read_only_allow_delete']:
        if setting in blocks_settings and blocks_settings[setting] == 'true':
            current_app.elasticsearch.indices.put_settings(index=index, body={
                'index': {"blocks.read_only": False,
                          "blocks.write": False,
                          "blocks.read_only_allow_delete": None}})
