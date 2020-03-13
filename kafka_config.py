base_config = {
    'client_id': 'leung',
    'request_timeout_ms': 3000,
}

store_config = {
    'db': 'mongodb',

    'mongo_connect': {
        'database': 'kafka',
        'collection': '_offset',
    },

    'offset': 'both'
}

create_topic_config = {
    'timeout_ms': 3000,
    'validate_only': False
}

producter_config = {
    'acks': 'all',
    'compression_type': 'gzip',
    'retries': 1,
    'linger_ms': 5,
    'max_request_size': 1048576,
    'buffer_memory': 33554432,
}

consumer_config = {

    'consumer_timeout_ms': 50,  # 如果在指定时间（ms）间隔后没有消息可供使用，则向使用方抛出超时异常
    'enable_auto_commit': False
}

admin_client_config = {

}

client_config = {

}
