import logging


class Encrypter(object):
    def __init__(self, beaver_config, filename, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.filename = filename
        self._initialize_from_config(beaver_config, filename)

    @classmethod
    def get_instance(cls, beaver_config, filename, logger=None):
        return cls(beaver_config, filename, logger=logger)

    def encrypt(self, message):
        return message

    def decrypt(self, message):
        return message

    def _initialize_from_config(self, beaver_config, filename):
        pass
