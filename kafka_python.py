import kafka
import logging
from kafka.errors import KafkaError
from functools import wraps
from kafka import KafkaClient, KafkaAdminClient, KafkaProducer, KafkaConsumer
from ipproxy_pool.config.kafka_config import base_config, \
    admin_client_config, \
    client_config, create_topic_config, \
    producter_config, consumer_config

from ipproxy_pool.db.MongodbManager import mongodbManager
from ipproxy_pool.config.kafka_config import store_config

create_topic_fail_code = 411
delete_topic_fail_code = -411
product_topic_fail_code = 401
consumer_topic_fail_code = -401


def singleton(cls):
    instances = {}

    @wraps(cls)
    def get_instance(**kwargs):
        if cls not in instances:
            instances[cls] = cls(**kwargs)
        return instances[cls]

    return get_instance


_logger = logging.getLogger('kafka-python')


class KafkaPython(object):

    def __init__(self, servers=None, client_id=None, request_timeout_ms=3000):
        if servers is None:
            servers = base_config.get('bootstrap_servers', ['localhost:9092'])

        self._bootstrap_servers = servers
        self._client_id = base_config.get('client_id', client_id)
        self._request_timeout_ms = base_config.get('request_timeout_ms', request_timeout_ms)

    @staticmethod
    def _log_msg(code, client_id, msg):
        return 'code:%s client_id %s reason:%s' % (code, client_id, msg)


@singleton
class Product(KafkaPython):
    def __init__(self, **kwargs):
        super().__init__()
        producter_config.update(kwargs)
        self.engine = KafkaProducer(bootstrap_servers=self._bootstrap_servers,
                                    client_id=self._client_id,
                                    request_timeout_ms=self._request_timeout_ms,
                                    **producter_config)

    def product_send(self, topic='', key=None, value=None, successCall=None, errorCall=None, **kwargs):

        try:
            self.engine.send(topic=topic, key=key, value=value, **kwargs).add_callback(successCall).add_errback(
                errorCall)
        except KafkaError as e:
            _logger.error(self._log_msg(product_topic_fail_code, self._client_id, msg='%s' % e))
            return
        self.engine.close(5)


@singleton
class Consumer(KafkaPython):

    def __init__(self, bootstrap_servers=None, **kwargs):

        super().__init__(servers=bootstrap_servers)
        consumer_config.update(kwargs)

        self.engine = KafkaConsumer(bootstrap_servers=self._bootstrap_servers,
                                    client_id=self._client_id,
                                    **consumer_config)

        self.group_id = consumer_config.get('group_id', None)
        self.tps = []
        self._partition_mode = None

    def get_user_topics(self):
        t = self.engine.topics()
        self.engine.close()
        return t

    def assign_partition(self, topics: list):

        if topics:

            for v in topics:
                tp = kafka.TopicPartition(topic=str(v['topic']), partition=int(v['partition']))
                self.tps.append(tp)
            self.engine.assign(self.tps)

        self._partition_mode = '1'
        return self

    def sub_partition(self, topic: list):
        self.tps = topic
        self._partition_mode = '2'
        return self

    def topic_consumer(self, **kwargs):
        if self._partition_mode == '1':

            for tp in self.tps:
                data = self.find_or_update(topic=tp.topic,
                                           partition=tp.partition,
                                           group_id=self.group_id,
                                           offset=self.engine.end_offsets([tp])[tp],
                                           )
                if data:
                    self.engine.seek(tp, data['offset'])
                else:

                    self.engine.seek(tp, self.engine.beginning_offsets([tp])[tp])

        elif self._partition_mode == '2':
            self.engine.subscribe(self.topics, pattern=kwargs['pattern'], listener=kwargs['listener'])
        else:
            raise Exception('you have to assign the partition mode')

        return self.engine

    @staticmethod
    def find_or_update(topic, partition, group_id, offset):
        client = Mongo()
        data = client.get_offset(topic=topic, partition=partition, group_id=group_id)
        if data is None:
            client.commit_offset(topic=topic,
                                 partition=partition,
                                 group_id=group_id,
                                 offset=offset,
                                 )
            return False
        else:
            client.update_offset(topic=topic, partition=partition, group_id=group_id, offset=offset)
            return data


@singleton
class AdminClient(KafkaPython):
    def __init__(self, **kwargs):
        super().__init__()
        admin_client_config.update(kwargs)
        self.engine = KafkaAdminClient(bootstrap_servers=self._bootstrap_servers,
                                       client_id=self._client_id,
                                       request_timeout_ms=self._request_timeout_ms,
                                       **admin_client_config)

    def create_topics(self, topic_list: list):
        new_topic = []

        for k, v in enumerate(topic_list):
            new_topic.append(NewTopics(name=v['name'],
                                       num_partitions=v['num_partitions'],
                                       replication_factor=v['replication_factor'],
                                       replica_assignments=v['replica_assignments'],
                                       topic_configs=v['topic_configs']))

        if not Consumer().get_user_topics().intersection({item['name'] for i, item in enumerate(topic_list)}):

            try:
                self.engine.create_topics(new_topic, **create_topic_config)
            except KafkaError as e:
                _logger.error(e)
            except Exception as e:
                _logger.error(e)

            self.engine.close()
        else:
            _logger.error(self._log_msg(create_topic_fail_code, self._client_id, 'topic重复'))
            return

    def delete_topics(self, topic: list):
        if Consumer().get_user_topics().intersection(set(topic)):

            try:
                self.engine.delete_topics(topic, self._request_timeout_ms)

            except KafkaError as e:
                _logger.error(e)

            self.engine.close()
        else:
            _logger.error(self._log_msg(delete_topic_fail_code, self._client_id, '需删除的topic不存在'))
            return


@singleton
class Client(KafkaPython):
    def __init__(self, bootstrap_servers=None, **kwargs):
        super().__init__(servers=bootstrap_servers)
        client_config.update(kwargs)
        self.engine = KafkaClient(bootstrap_servers=self._bootstrap_servers,
                                  client_id=self._client_id,
                                  request_timeout_ms=self._request_timeout_ms,
                                  **client_config)


class NewTopics(object):
    def __init__(self, name, num_partitions, replication_factor, replica_assignments={}, topic_configs={}):
        self.name = name
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.replica_assignments = replica_assignments
        self.topic_configs = topic_configs


commit_fail_code = -7001
update_fail_code = -7002


class Mongo(object):

    def __init__(self):
        self.database = store_config['mongo_connect']['database']
        self.collection = store_config['mongo_connect']['collection']
        self.client = mongodbManager(database=self.database, collection=self.collection).mongo_collection()

    def get_offset(self, topic, partition, group_id):
        return self.client.find_one({'topic': topic, 'partition': partition, 'group_id': group_id})

    def update_offset(self, topic, partition, group_id, offset):
        try:
            self.client.update({'topic': topic, 'partition': partition, 'group_id': group_id},
                               {"$set": {'offset': offset}}
                               )
        except Exception as e:
            _logger.error(
                'code:{0} , topic:{1} , partition:{2} , group_id:{3} commit offset fail:{4}'.format(update_fail_code,
                                                                                                    topic, partition,
                                                                                                    group_id, e))

    def commit_offset(self, topic, partition, group_id, offset):
        try:
            return self.client.insert_one(
                {'topic': topic, 'partition': partition, 'group_id': group_id, 'offset': offset, })
        except Exception as e:
            _logger.error(
                'code:{0} , topic:{1} , partition:{2} , group_id:{3} commit offset fail:{4}'.format(commit_fail_code,
                                                                                                    topic, partition,
                                                                                                    group_id, e))
