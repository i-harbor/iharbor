import json

import requests


class RabbitMQTool(object):
    def __init__(self, host, queue, user, passwd):
        self.host = host
        self.queue = queue
        self.user = user
        self.passwd = passwd

    def _get_api_data(self):
        """
        return: list object of requests response
        """
        r = requests.get(url=self.host + "/api/queues", auth=(self.user, self.passwd))
        if r.status_code != 200:
            self.resp = {}
        else:
            self.resp = json.loads(r.text)

    def _get_msg_length(self):
        """
        return: unprocessed msgs
        """

        for i in self.resp:
            if i['name'] == self.queue:
                return int(i['messages_unacknowledged']) + int(i['messages_ready'])
            pass
        return None

    def _get_msg_rate(self):
        """
        return: publish rate, ack rate
        """
        for i in self.resp:
            if i['name'] == self.queue:
                return int(i['message_stats']['publish_details']['rate']), int(
                    i['message_stats']['ack_details']['rate'])
            pass
        return 0, 0

    def set_sleep(self):
        """
        descr: base on eta
        """
        baseline = 3000  # 设置基线
        pub_rate, ack_rate = self._get_msg_rate()
        diff = ack_rate - pub_rate
        length = self._get_msg_length()
        if length < baseline:
            return 0
        #  ack_rate > pub_rate -> queue--
        return (length - baseline) // ack_rate

    def refresh(self):
        """
        refresh status
        """
        self._get_api_data()
        return self.set_sleep()
