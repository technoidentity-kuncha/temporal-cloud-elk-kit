"""Test-only encryption codec: a fresh key per test, never production credentials."""

from cryptography.fernet import Fernet
from temporalio.api.common.v1 import Payload
from temporalio.converter import PayloadCodec


class EncryptedPayloadCodec(PayloadCodec):
    def __init__(self):
        self.cipher = Fernet(Fernet.generate_key())

    async def encode(self, payloads):
        return [Payload(metadata={"encoding": b"binary/encrypted"},
                        data=self.cipher.encrypt(p.SerializeToString())) for p in payloads]

    async def decode(self, payloads):
        return [Payload.FromString(self.cipher.decrypt(p.data))
                if p.metadata.get("encoding") == b"binary/encrypted" else p for p in payloads]
