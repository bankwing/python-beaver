import sys
if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest

from mock import MagicMock, patch
from beaver.encrypters import KmsEncrypter
import botocore.session
import botocore.client


@unittest.skipIf(sys.version_info[:2] <= (2, 6), 'KMS encryption only supported in python >= 2.7')
class TestKmsEncryptor(unittest.TestCase):
    def setUp(self):
        self.key_id = 'arn:aws:kms:us-east-1:012345678901:key/testkey'
        self.mock_beaver_config = MagicMock()
        self.kms_client_patch = patch('botocore.session.Session', spec=botocore.session.Session)
        mock_session = self.kms_client_patch.start()
        mock_session.return_value.user_agent_name = 'TestKms'
        self.mock_client = MagicMock(spec=botocore.client.BaseClient)
        mock_session.return_value.create_client.return_value = self.mock_client
        self.mock_client.meta = MagicMock()

    def tearDown(self):
        self.kms_client_patch.stop()

    def test_instance_caching(self):
        self.mock_beaver_config.get_field.return_value = None
        self._beaver_config_returns('a', 'b', self.key_id, 'foo=bar')
        KmsEncrypter._instance_cache = {}
        instance = KmsEncrypter.get_instance(self.mock_beaver_config, 'foo')
        self.assertEqual(1, len(KmsEncrypter._instance_cache))
        self.assertEqual(instance, KmsEncrypter.get_instance(self.mock_beaver_config, 'foo'))
        self.assertEqual(1, len(KmsEncrypter._instance_cache))

        self._beaver_config_returns('a', 'b', self.key_id + ', ' + self.key_id + 'bar', 'foo=bar')
        instance = KmsEncrypter.get_instance(self.mock_beaver_config, 'foo')
        self.assertEqual(2, len(KmsEncrypter._instance_cache))
        self.assertEqual(instance, KmsEncrypter.get_instance(self.mock_beaver_config, 'foo'))
        self.assertEqual(2, len(KmsEncrypter._instance_cache))

        self._beaver_config_returns('a', 'b', self.key_id + 'bar' + ', ' + self.key_id, 'foo=bar')
        self.assertEqual(2, len(KmsEncrypter._instance_cache))
        self.assertEqual(instance, KmsEncrypter.get_instance(self.mock_beaver_config, 'foo'))
        self.assertEqual(2, len(KmsEncrypter._instance_cache))

        self._beaver_config_returns('a', 'c', self.key_id + ', ' + self.key_id + 'bar', 'foo=bar')
        instance = KmsEncrypter.get_instance(self.mock_beaver_config, 'foo')
        self.assertEqual(3, len(KmsEncrypter._instance_cache))
        self.assertEqual(instance, KmsEncrypter.get_instance(self.mock_beaver_config, 'foo'))
        self.assertEqual(3, len(KmsEncrypter._instance_cache))

        self._beaver_config_returns('a', 'd', self.key_id + ', ' + self.key_id + 'bar', 'foo=bar')
        instance = KmsEncrypter.get_instance(self.mock_beaver_config, 'foo')
        self.assertEqual(4, len(KmsEncrypter._instance_cache))
        self.assertEqual(instance, KmsEncrypter.get_instance(self.mock_beaver_config, 'foo'))
        self.assertEqual(4, len(KmsEncrypter._instance_cache))

        self._beaver_config_returns('a', 'd', self.key_id + ', ' + self.key_id + 'bar', 'foo2=bar')
        instance = KmsEncrypter.get_instance(self.mock_beaver_config, 'foo')
        self.assertEqual(5, len(KmsEncrypter._instance_cache))
        self.assertEqual(instance, KmsEncrypter.get_instance(self.mock_beaver_config, 'foo'))
        self.assertEqual(5, len(KmsEncrypter._instance_cache))

        self._beaver_config_returns('a', 'b', self.key_id, 'foo=bar', cache_age_seconds=1)
        instance = KmsEncrypter.get_instance(self.mock_beaver_config, 'foo')
        self.assertEqual(6, len(KmsEncrypter._instance_cache))
        self.assertEqual(instance, KmsEncrypter.get_instance(self.mock_beaver_config, 'foo'))
        self._beaver_config_returns('a', 'b', self.key_id, 'foo=bar', cache_age_seconds=1)
        self.assertEqual(6, len(KmsEncrypter._instance_cache))

        self._beaver_config_returns('a', 'b', self.key_id, 'foo=bar', cache_age_seconds=1, cache_capacity=1)
        instance = KmsEncrypter.get_instance(self.mock_beaver_config, 'foo')
        self.assertEqual(7, len(KmsEncrypter._instance_cache))
        self.assertEqual(instance, KmsEncrypter.get_instance(self.mock_beaver_config, 'foo'))
        self._beaver_config_returns('a', 'b', self.key_id, 'foo=bar', cache_age_seconds=1)
        self.assertEqual(7, len(KmsEncrypter._instance_cache))

    def test_encrypt_then_decrypt(self):
        self.mock_beaver_config.get_field.return_value = None
        self._beaver_config_returns('a', 'b', self.key_id, None)
        instance = KmsEncrypter.get_instance(self.mock_beaver_config, 'foo')

        self.mock_client.generate_data_key = MagicMock()
        self.mock_client.generate_data_key_without_plaintext = MagicMock()
        self.mock_client.encrypt = MagicMock()
        self.mock_client.decrypt = MagicMock()

        self.mock_client.generate_data_key.return_value = {
            u'Plaintext': "R\x9131\x1e\xd0\xdd$\x16\x90\x13t\xcd\xd7\x1c\xef\x03@\xf5k\xb9\xbf\xc3t''\x8d\n\xcbH\xf2J",
            u'KeyId': self.key_id,
            'ResponseMetadata': {
                'RetryAttempts': 0,
                'HTTPStatusCode': 200,
            },
            u'CiphertextBlob': ('\x01\x01\x03\x00x<\r\xbf\xde,\xa9\xc9\xb8\x98\x14f\xf2.h@\xed\xa8q\xd7\xdf' +
                                '\xc0NtL\xed\x10a\x1e=)s\x1c\x00\x00\x00~0|\x06\t*\x86H\x86\xf7\r\x01\x07\x06' +
                                '\xa0o0m\x02\x01\x000h\x06\t*\x86H\x86\xf7\r\x01\x07\x010\x1e\x06\t`\x86H\x01e' +
                                '\x03\x04\x01.0\x11\x04\x0cRN\xf5\xd48\xe6\x02\x9dn;\xef\xac\x02\x01\x10\x80;' +
                                '\xb2\xd1\x90l\x9b9\xba`j\x1d6/\x0eJy\xe4\x11\xf2\xd7\x13\xb7*\\\x87dO\xd2(*\xe5' +
                                '\xc7M\xb12\xf7\xb2\xbb\xf5!\x86\r5\xe4\x1cx\x87\xd7yO\x89\xf99\xef*.\xc4\x00' +
                                '\xf3\x0f')
        }
        self.mock_client.generate_data_key_without_plaintext.return_value = \
            self.mock_client.generate_data_key.return_value

        self.mock_client.encrypt.return_value = \
            self.mock_client.generate_data_key.return_value

        self.mock_client.decrypt.return_value = \
            self.mock_client.generate_data_key.return_value

        message = "Hello World"
        encrypted = instance.encrypt(message)
        self.assertEqual(message, instance.decrypt(encrypted))

    def _beaver_config_returns(self, kms_access_key, kms_secret_key, kms_key_ids, kms_encryption_context,
                               cache_capacity=None, cache_age_seconds=None):
        def get(key, *args, **kwargs):
            if key == 'aws_kms_access_key':
                return kms_access_key
            elif key == 'aws_kms_secret_key':
                return kms_secret_key
            elif key == 'aws_kms_key_ids':
                return kms_key_ids
            elif key == 'aws_kms_encryption_context':
                return kms_encryption_context
            elif key == 'aws_kms_cache_age_seconds':
                return cache_age_seconds
            elif key == 'aws_kms_cache_capacity':
                return cache_capacity
            else:
                return None

        self.mock_beaver_config.get.side_effect = get
