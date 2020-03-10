import kafka
import logging
from kafka.errors import KafkaError
from functools import wraps
from kafka import KafkaClient, KafkaAdminClient, KafkaProducer, KafkaConsumer
from ipproxy_pool.config.kafka_config import base_config, \
    admin_client_config, \
    client_config, create_topic_config, \
    producter_config, consumer_config
from ipproxy_pool.db.model.KafkaQueueModel import Mongo

create_topic_fail_code = 411
delete_topic_fail_code = -411
product_topic_fail_code = 401
consumer_topic_fail_code = -401
product_topic_not_exist = -404


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
    def _logMsg(code, client_id, msg):
        return 'code:%s client_id %s reason:%s' % (code, client_id, msg)


@singleton
class Product(KafkaPython):
    def __init__(self, bootstrap_servers=None, **kwargs):
        super().__init__(servers=bootstrap_servers)
        producter_config.update(kwargs)  # 可动态配置每个生产者实例
        self.engine = KafkaProducer(bootstrap_servers=self._bootstrap_servers,
                                    client_id=self._client_id,
                                    request_timeout_ms=self._request_timeout_ms,
                                    **producter_config)

        self.exist_topics = Consumer().get_user_topics()

    def product_send(self, topic='', key=None, value=None, successCall=None, errorCall=None, **kwargs):

        if topic not in self.exist_topics:
            msg = 'topic is not exist'
            _logger.warning(self._logMsg(product_topic_not_exist, self._client_id, msg))
            raise Exception(msg)

        try:
            self.engine.send(topic=topic, key=key, value=value, **kwargs).add_callback(successCall).add_errback(
                errorCall)

        except KafkaError as e:
            _logger.error(self._logMsg(product_topic_fail_code, self._client_id, msg='%s' % e))
            return


# 分配topic分区 or 订阅topic->seek调节位移->消费记录->提交offset
class Consumer(KafkaPython):

    def __init__(self, bootstrap_servers=None, **kwargs):
        super().__init__(servers=bootstrap_servers)
        consumer_config.update(kwargs)  # 可动态配置每个消费者实例

        self.engine = KafkaConsumer(bootstrap_servers=self._bootstrap_servers,
                                    client_id=self._client_id,
                                    **consumer_config)
        self.DbClient = Mongo()
        self.group_id = consumer_config.get('group_id', None)
        self.tps = []
        self._partition_mode = None
        self.offset_mode = 'both'  # 3种offset存储机制. 1db  2kafka 3both

    def get_user_topics(self):
        return self.engine.topics()

    # 消费模式1 : 手动分配分区对象给当前消费者
    def assign_partition(self, topics: list):
        if topics:
            for v in topics:
                tp = kafka.TopicPartition(topic=str(v['topic']), partition=int(v['partition']))
                self.__store_last_offset__(tp=tp)  # 存储上次消费的offset记录纸
                self.tps.append(tp)
            self.engine.assign(self.tps)

        self._partition_mode = '1'
        return self

    # 消费模式2 : 消费者主动订阅topic
    def sub_partition(self, topic: list):
        self.tps = topic
        self._partition_mode = '2'
        return self

    def topic_consumer(self, **kwargs):

        topic_messages = []
        if self._partition_mode == '1':

            for tp in self.tps:
                data = self.find_or_create(topic=tp.topic,
                                           partition=tp.partition,
                                           group_id=self.group_id,
                                           offset=self.engine.position(tp),
                                           )
                if data:
                    self.engine.seek(tp, data['offset'])
                else:
                    self.engine.seek(tp, self.engine.beginning_offsets([tp])[tp])

            for msg in self.engine:
                self._commit_offset(group_id=self.group_id, topic=msg.topic, partition=msg.partition, offset=msg.offset)
                msg.value.update({'timestamp': msg.timestamp, "offset": msg.offset})  # 加多2个排序数据方便排序
                topic_messages.append(msg.value)
                # if msg.offset == 18:
                #     self.engine.close()
            return topic_messages

        elif self._partition_mode == '2':
            self.engine.subscribe(self.tps, pattern=kwargs.get('pattern', None),
                                  listener=kwargs.get('listener', None))
        else:
            raise Exception('you have to chose the partition mode')

    @staticmethod
    def find_or_create(topic, partition, group_id, offset):
        client = Mongo()
        data = client.get_offset(topic=topic, partition=partition, group_id=group_id)
        if data is None:
            client.commit_offset(topic=topic,
                                 partition=partition,
                                 group_id=group_id,
                                 offset=offset,
                                 )
        else:
            return data

    # 当前消费者的offset信息提交函数
    def _commit_offset(self, group_id, topic, partition, offset):
        if self.group_id is None:
            raise Exception('you must enter an group_id')

        tp = kafka.TopicPartition(topic=str(topic), partition=int(partition))

        # 分别提交offset信息至kafka and database
        if self.offset_mode == 'both':

            self.engine.commit(offsets={tp: (kafka.OffsetAndMetadata(offset, None))})
            self.DbClient.commit_offset(topic=topic, group_id=group_id, partition=partition, offset=offset)
        # 提交至kafka服务器
        elif self.offset_mode == 'kafka':
            self.engine.commit(offsets={tp: (kafka.OffsetAndMetadata(offset, None))})
        # 提交至database
        else:
            self.DbClient.commit_offset(topic=topic, group_id=group_id, partition=partition, offset=offset)

    # 当消费者完成消费过程并提交数据至业务并且业务日常GG了
    # 依赖mongodb
    def _rollback_offset(self, topic, partition, group_id):
        offset = self.DbClient.get_offset(topic, partition, group_id, mode='_back')['offset']
        self._commit_offset(group_id=group_id, topic=topic, partition=partition, offset=offset)

    # 消费端超时(或down机)的最后消费记录存储
    def __store_last_offset__(self, tp=None):
        offset = self.engine.position(tp)
        self.DbClient.last_offset_save(topic=tp['topic'], partition=tp['partition'], offset=offset,
                                       group_id=self.group_id)


@singleton
class AdminClient(KafkaPython):
    def __init__(self, bootstrap_servers=None, **kwargs):
        super().__init__(servers=bootstrap_servers)
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
            _logger.error(self._logMsg(create_topic_fail_code, self._client_id, 'topic重复'))
            return

    def delete_topics(self, topic: list):
        if Consumer().get_user_topics().intersection(set(topic)):

            try:
                self.engine.delete_topics(topic, self._request_timeout_ms)

            except KafkaError as e:
                _logger.error(self._logMsg(delete_topic_fail_code, self._client_id, '删除的topic失败:%s' % e))

            self.engine.close()
        else:
            _logger.error(self._logMsg(delete_topic_fail_code, self._client_id, '需删除的topic不存在'))
            return


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
