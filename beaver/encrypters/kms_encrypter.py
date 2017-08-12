import sys
from threading import Lock

from beaver.encrypters.base_encrypter import Encrypter

if sys.version_info >= (2, 7):
    import aws_encryption_sdk
    import botocore.session
else:
    aws_encryption_sdk = False


class KmsConfigValues(object):
    def __init__(self,
                 aws_kms_access_key=None,
                 aws_kms_secret_key=None,
                 aws_kms_key_ids=[],
                 aws_kms_encryption_context=None,
                 cache_capacity=None,
                 cache_age_seconds=None):
        self.aws_kms_access_key = aws_kms_access_key or None
        self.aws_kms_secret_key = aws_kms_secret_key or None
        self.aws_kms_key_ids = aws_kms_key_ids or []
        self.aws_kms_encryption_context = aws_kms_encryption_context or {}
        self.cache_capacity = int(cache_capacity or 100)
        self.cache_age_seconds = float(cache_age_seconds or 300)

    def __eq__(self, other):
        return isinstance(other, KmsConfigValues) and \
               self.aws_kms_access_key == other.aws_kms_access_key and \
               self.aws_kms_secret_key == other.aws_kms_secret_key and \
               set(self.aws_kms_key_ids) == set(other.aws_kms_key_ids) and \
               self.aws_kms_encryption_context == other.aws_kms_encryption_context and \
               self.cache_age_seconds == other.cache_age_seconds and \
               self.cache_capacity == other.cache_capacity

    def __hash__(self):
        return hash((self.aws_kms_access_key,
                     self.aws_kms_secret_key,
                     tuple(sorted(self.aws_kms_key_ids)),
                     tuple([(k, v) for k, v in self.aws_kms_encryption_context.iteritems()]),
                     self.cache_capacity,
                     self.cache_age_seconds))


class KmsEncrypter(Encrypter):
    _instance_cache = {}
    _instance_cache_lock = Lock()

    @classmethod
    def get_instance(cls, beaver_config, filename, logger=None):
        kms_configs = _get_kms_config_values(beaver_config, filename)

        with cls._instance_cache_lock:
            instance = cls._instance_cache.get(kms_configs, None) or cls(beaver_config, filename, logger=logger)
            cls._instance_cache[kms_configs] = instance

        return cls._instance_cache[kms_configs]

    def _initialize_from_config(self, beaver_config, filename):
        super(KmsEncrypter, self)._initialize_from_config(beaver_config, filename)

        if not aws_encryption_sdk:
            raise SystemError('The use_aws_kms_encryption option is only supported for python >= 2.7')

        kms_configs = _get_kms_config_values(beaver_config, filename)
        kms_secret_key = kms_configs.aws_kms_secret_key
        kms_access_key = kms_configs.aws_kms_access_key
        kms_key_ids = kms_configs.aws_kms_key_ids

        self.kms_encryption_context = kms_configs.aws_kms_encryption_context

        if not kms_key_ids:
            raise ValueError('You must provide at lease one CMS key for aws_kms_key_ids!')

        if self.kms_encryption_context and not isinstance(self.kms_encryption_context, dict):
            raise ValueError('kms_encryption_context must be a flat map of key value pairs, or not provided.')

        session = botocore.session.Session()

        if kms_access_key and kms_secret_key:
            session.set_credentials(kms_access_key, kms_secret_key)
        else:
            self.logger.info('Using default boto credentials locations for KMS access.')

        raw_key_provider = aws_encryption_sdk.KMSMasterKeyProvider(botocore_session=session,
                                                                   key_ids=kms_key_ids)
        cache = aws_encryption_sdk.LocalCryptoMaterialsCache(capacity=kms_configs.cache_capacity)
        self.materials_manager = aws_encryption_sdk.CachingCryptoMaterialsManager(
            master_key_provider=raw_key_provider,
            cache=cache,
            max_age=kms_configs.cache_age_seconds)

    def encrypt(self, message):
        return aws_encryption_sdk.encrypt(**self._get_kms_args(message))[0]

    def decrypt(self, message):
        return aws_encryption_sdk.decrypt(**self._get_kms_args(message))[0]

    def _get_kms_args(self, message):
        kms_args = {
            'source': message,
            'materials_manager': self.materials_manager,
        }
        if self.kms_encryption_context:
            kms_args['encryption_context'] = self.kms_encryption_context

        return kms_args


def _get_kms_config_values(beaver_config, filename):
    kms_access_key = _load_config(beaver_config, 'aws_kms_access_key', filename) or None
    kms_secret_key = _load_config(beaver_config, 'aws_kms_secret_key', filename) or None
    cache_capacity = _load_config(beaver_config, 'aws_kms_cache_capacity', filename) or None
    cache_age_seconds = _load_config(beaver_config, 'aws_kms_cache_age_seconds', filename) or None

    kms_key_ids = _load_config(beaver_config, 'aws_kms_key_ids', filename) or ''
    kms_key_ids = [s.strip() for s in kms_key_ids.split(',')] or []

    kms_encryption_context_string = _load_config(beaver_config, 'aws_kms_encryption_context', filename)
    kms_encryption_context = {}
    if kms_encryption_context_string:
        for item in kms_encryption_context_string.split(','):
            if '=' not in item:
                raise ValueError('Improperly formed aws_kms_encryption_context.')
            parts = item.strip().split('=')
            if len(parts) != 2:
                raise ValueError('Improperly formed aws_kms_encryption_context.')

            kms_encryption_context[parts[0].strip()] = parts[1].strip()

    return KmsConfigValues(kms_access_key, kms_secret_key, kms_key_ids, kms_encryption_context,
                           cache_capacity, cache_age_seconds)


def _load_config(beaver_config, field, filename):
    return beaver_config.get_field(field, filename) or beaver_config.get(field, None)
