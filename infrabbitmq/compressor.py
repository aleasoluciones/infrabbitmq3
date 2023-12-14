import gzip

from infcommon import logger


class Compressor:
    def compress(self, message, compression_enable):
        if not compression_enable:
            return message
        try:
            return gzip.compress(message.encode()) if isinstance(message, str) else gzip.compress(message)
        except Exception as exc:
            logger.error(f"Error compressing {message} with exception {exc}. Using original message")
            return message

    def decompress(self, message, compression_enable):
        if not compression_enable:
            return message
        try:
            return gzip.decompress(message) if isinstance(message, bytes) else message
        except Exception as exc:
            logger.error(f"Error decompressing {message} with exception {exc}. Using original message")
            return message
