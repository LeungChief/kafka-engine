from kafka.errors import KafkaError
from functools import wraps
import logging, json
from kafka import KafkaClient, KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka_config import base_config, \
    admin_client_config, \
    client_config, create_topic_config, \
    producter_config, consumer_config

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


class KafkaPython(object):

    def __init__(self, servers=None, client_id='', request_timeout_ms=3000):
        if servers is None:
            servers = base_config.get('bootstrap_servers', ['localhost:9092'])

        self._bootstrap_servers = servers
        self._client_id = base_config.get('client_id', client_id)
        self._request_timeout_ms = base_config.get('request_timeout_ms', request_timeout_ms)
        self._logger = logging.getLogger('kafka-python')

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
            self._logger.error(self._log_msg(product_topic_fail_code, self._client_id, msg='%s' % e))
            return
        self.engine.close(5)


@singleton
class Consumer(KafkaPython):

    def __init__(self, bootstrap_servers=None, **kwargs):
        super().__init__(servers=bootstrap_servers)
        consumer_config.update(kwargs)
        self.engine = KafkaConsumer(bootstrap_servers=self._bootstrap_servers,
                                    client_id=self._client_id,
                                    request_timeout_ms=self._request_timeout_ms,
                                    **consumer_config)

    def get_user_topics(self):
        return self.engine.topics()


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
                self._logger.error(e)
            except Exception as e:
                self._logger.error(e)

            self.engine.close()
        else:
            self._logger.error(self._log_msg(create_topic_fail_code, self._client_id, 'topic重复'))
            return

    def delete_topics(self, topic: list):
        if Consumer().get_user_topics().intersection(set(topic)):

            try:
                self.engine.delete_topics(topic, self._request_timeout_ms)

            except KafkaError as e:
                self._logger.error(e)

            self.engine.close()
        else:
            self._logger.error(self._log_msg(delete_topic_fail_code, self._client_id, '需删除的topic不存在'))
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
