from mamba import description, context, it, before
from expects import expect, equal

import gzip

from infrabbitmq import compressor

A_TOPIC_PREFIX = 'a_topic_prefix'
A_NETWORK = 'a_network'
AN_EVENT_NAME = 'an_event_name'

with description('Compressor tests:') as self:
    with before.each:
        self.compressor = compressor.Compressor()

    with context('compress'):
        with context('and compression is not enabled'):
            with it('returns original message'):
                a_message = 'a_message'

                result = self.compressor.compress(a_message, False)

                expect(result).to(equal(a_message))

        with context('and compression is enabled'):
            with context('and it is a string'):
                with it('returns compressed message'):
                    a_message = 'a_message'

                    result = self.compressor.compress(a_message, True)

                    expect(result).to(equal(gzip.compress(a_message.encode())))

            with context('and it is a byte object'):
                with it('returns compressed message'):
                    a_message = b'a_message'

                    result = self.compressor.compress(a_message, True)

                    expect(result).to(equal(gzip.compress(a_message)))

    with context('decompress'):
        with context('and compression is not enabled'):
            with it('returns original message'):
                a_message = 'a_message'

                result = self.compressor.decompress(a_message, False)

                expect(result).to(equal(a_message))

        with context('and compression is enabled'):
            with it('returns decompressed message'):
                a_message = b'a_message'
                a_compressed_message = gzip.compress(a_message)

                result = self.compressor.decompress(a_compressed_message, True)

                expect(result).to(equal(a_message))

            with context('having errors decompressig'):
                with it('returns orginal message'):
                    a_message = 'a_message'

                    result = self.compressor.decompress(a_message, True)

                    expect(result).to(equal(a_message))
