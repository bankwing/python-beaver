from beaver.encrypters.base_encrypter import Encrypter
from beaver.encrypters.kms_encrypter import KmsEncrypter


ENCRYPTERS = {
    'kms': KmsEncrypter,
    'KMS': KmsEncrypter,
    'default': Encrypter
}
