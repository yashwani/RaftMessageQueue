import queue


class MessageQueue:
    def __init__(self):
        self.data = {}

    def process(self, message):
        if message["type"] == "topic" and message["method"] == "PUT":
            return self.put_topic(message)
        if message["type"] == "topic" and message["method"] == "GET":
            return self.get_topics()
        if message["type"] == "message" and message["method"] == "PUT":
            return self.put_message(message)
        if message["type"] == "message" and message["method"] == "GET":
            print("Getting message from " + str(message['topic']))
            return self.get_message(message)

    def put_topic(self, message):
        m_topic = message['topic']
        if m_topic not in self.data:
            self.data[m_topic] = queue.Queue()
            #print("reply 1 ######")
            return {'success': True}
        else:
            return {'success': False}

    def get_topics(self):
        return {'success': True, 'topics': list(self.data.keys())}

    def put_message(self, message):
        """
            Add message to the appropriate topic.
            If the topic doesn't exist, return False
        """
        m_topic = message['topic']
        if m_topic not in self.data.keys():
            return {'success': False}
        else:
            self.data[m_topic].put(message['message'])
            #print("reply 2 #############")
            return {'success': True}

    def get_message(self, message):
        """
            If passed in request, use request fields to get topic message
        """
        m_topic = message['topic']
        if m_topic not in self.data.keys():
            return {'success': False}
        if self.data[m_topic].empty():
            return {'success': False}
        msg = self.data[m_topic].get()
        return {'success': True, 'message': msg}

    def get_data(self):
        return self.data
