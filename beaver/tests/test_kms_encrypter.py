import unittest
from mock import MagicMock

from moto import mock_kms
import boto3

from beaver.encrypters import KmsEncrypter


class TestKmsEncryptor(unittest.TestCase):
    def __call__(self, *args, **kwargs):
        with mock_kms():
            super(TestKmsEncryptor, self).__call__(*args, **kwargs)

    def setUp(self):
        kms = boto3.client('kms', region_name='us-east-1')
        key_result = kms.create_key()
        self.key_id = key_result['KeyMetadata']['Arn']
        self.mock_beaver_config = MagicMock()

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

    @unittest.skip('Broken because moto doesnt implement all the needed KMS APIs')
    def test_encrypt_then_decrypt(self):
        self.mock_beaver_config.get_field.return_value = None
        self._beaver_config_returns('a', 'b', self.key_id, 'foo=bar')
        instance = KmsEncrypter.get_instance(self.mock_beaver_config, 'foo')

        message = "Hello World"
        encrypted = instance.encrypt(message)
        self.assertEqual(message, instance.decrypt(encrypted))

    def _beaver_config_returns(self, kms_access_key, kms_secret_key, kms_key_ids, kms_encryption_context):
        def get(key, *args, **kwargs):
            if key == 'aws_kms_access_key':
                return kms_access_key
            if key == 'aws_kms_secret_key':
                return kms_secret_key
            if key == 'aws_kms_key_ids':
                return kms_key_ids
            if key == 'aws_kms_encryption_context':
                return kms_encryption_context

        self.mock_beaver_config.get.side_effect = get
