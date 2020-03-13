import kafka
import logging
from kafka.errors import KafkaError
from functools import wraps
from kafka import KafkaClient, KafkaAdminClient, KafkaProducer, KafkaConsumer
from ipproxy_pool.config.kafka_config import base_config, \
    admin_client_config, \
    client_config, create_topic_config, \
    producter_config, consumer_config, store_config
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

    def __init__(self, servers=None, request_timeout_ms=3000):
        if servers is None:
            servers = base_config.get('bootstrap_servers', ['localhost:9092'])
        self._client_id = base_config.get('client_id')
        self._bootstrap_servers = servers
        self._request_timeout_ms = base_config.get('request_timeout_ms', request_timeout_ms)

    @staticmethod
    def _logMsg(code, client_id, msg):
        return 'code:%s client_id %s reason:%s' % (code, client_id, msg)

    def set_clientId(self, client_id=None):
        self._client_id = client_id
        return self


class Product(KafkaPython):
    def __init__(self, bootstrap_servers=None, **kwargs):
        super().__init__(servers=bootstrap_servers)
        producter_config.update(kwargs)
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
            send = self.engine.send(topic=topic, key=key, value=value, **kwargs)
            if successCall is not None:
                send.add_callback(successCall)
            if errorCall is not None:
                send.add_errback(errorCall)
        except KafkaError as e:
            _logger.error(self._logMsg(product_topic_fail_code, self._client_id, msg='%s' % e))
            return


# 分配topic分区 or 订阅topic->seek调节位移->消费记录->提交offset
# 1.第一次消费: 创建消费组消费记录.从第一个offset开始消费-> 并边消费边实时更新current_offset至当前消费值
# 2.第N次消费: 从current_offset值开始消费.边消费边实时更新current_offset至当前消费值
# 3.当消费端的其他业务GG. 可以在业务try捕获异常的处理中调用rollback_commit 回滚current_offset至最后一次正常消费值或者指定消费值,
class Consumer(KafkaPython):

    def __init__(self, bootstrap_servers=None, **kwargs):
        super().__init__(servers=bootstrap_servers)
        consumer_config.update(kwargs)

        self.engine = KafkaConsumer(bootstrap_servers=self._bootstrap_servers,
                                    client_id=self._client_id,
                                    **consumer_config)
        self.DbClient = Mongo()
        self.group_id = consumer_config.get('group_id', None)
        self.tps = []
        self._partition_mode = None
        self.offset_store_mode = store_config.get('offset')  # 3种offset存储机制. 1db  2kafka 3both

    def get_user_topics(self):
        return self.engine.topics()

    # 消费模式1 : 手动分配分区对象给当前消费者
    '''
    topics 参考值: {'name': 'test_topic', 'num_partitions': 3, 'replication_factor': 3, 'replica_assignments': {},'topic_configs': {}}
    '''
    def assign_partition(self, topics: list):
        if Consumer().get_user_topics().intersection({item['topic'] for i, item in enumerate(topics)}):
            for v in topics:
                tp = kafka.TopicPartition(topic=str(v['topic']), partition=int(v['partition']))
                self.tps.append(tp)
            self.engine.assign(self.tps)
        else:
            raise Exception('topics包含了未知的topic' % topics)
        self._partition_mode = '1'
        return self

    # 消费模式2 : 消费者主动订阅topic
    def sub_partition(self, topic: list):
        self.tps = topic
        self._partition_mode = '2'
        return self

    # 开始消费
    def topic_consumer(self, **kwargs):

        if self._partition_mode == '1':

            for tp in self.tps:

                data = self.find_or_create(topic=tp.topic,
                                           partition=tp.partition,
                                           group_id=self.group_id,
                                           )

                if data:
                    self.engine.seek(tp, int(data.get('current_offset', 0)) + 1)
                else:
                    self.engine.seek(tp, self.engine.beginning_offsets([tp])[tp])

            return self.engine

        elif self._partition_mode == '2':
            self.engine.subscribe(self.tps, pattern=kwargs.get('pattern', None),
                                  listener=kwargs.get('listener', None))
        else:
            raise Exception('you have to chose the partition mode')

    # 将current offset回滚至 指定offset Or committed
    # 当消费者完成消费过程并提交数据至业务并且业务日常GG了
    def rollback_offset(self, topic, partition, group_id, offset=None):
        if offset is None:
            committed_offset = self.engine.committed(kafka.TopicPartition(topic=topic, partition=partition))
            if committed_offset is None:
                raise Exception(
                    'topic:%s,partition:%s,group_id:%s has not commit record yet,you should enter some offset'
                    % (topic, partition, group_id))

        self.commit_offset(group_id=group_id, topic=topic, partition=partition, offset=offset)

    # 当前消费者的offset信息提交函数
    def commit_offset(self, group_id, topic, partition, offset):
        if self.group_id is None:
            raise Exception('you must enter an group_id')

        tp = kafka.TopicPartition(topic=str(topic), partition=int(partition))

        # 分别提交offset信息至kafka and database
        if self.offset_store_mode == 'both':

            self.engine.commit(offsets={tp: (kafka.OffsetAndMetadata(offset, None))})
            self.DbClient.commit_offset(topic=topic, group_id=group_id, partition=partition, offset=offset)
        # 提交至kafka服务器
        elif self.offset_store_mode == 'kafka':
            self.engine.commit(offsets={tp: (kafka.OffsetAndMetadata(offset, None))})
        # 提交至database
        else:
            self.DbClient.commit_offset(topic=topic, group_id=group_id, partition=partition, offset=offset)

    def find_or_create(self, **kwargs):
        client = self.DbClient
        data = client.get_offset(**kwargs)
        if data is None:
            client.create_offset(**kwargs)
            return False
        else:
            return data


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


@singleton
class Client(KafkaPython):
    def __init__(self, bootstrap_servers=None, **kwargs):
        super().__init__(servers=bootstrap_servers)
        client_config.update(kwargs)
        self.engine = KafkaClient(bootstrap_servers=self._bootstrap_servers,
                                  request_timeout_ms=self._request_timeout_ms,
                                  **client_config)


class NewTopics(object):
    def __init__(self, name, num_partitions, replication_factor, replica_assignments={}, topic_configs={}):
        self.name = name
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.replica_assignments = replica_assignments
        self.topic_configs = topic_configs
