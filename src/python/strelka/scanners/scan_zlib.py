import zlib

from strelka import strelka


class ScanZlib(strelka.Scanner):
    """Decompresses zlib files."""

    def scan(self, data, file, options, expire_at):
        uuid_part = str(file.name).split('___')[0] if '___' in str(file.name) else "unknown"

        try:
            # Decompress file and collect metadata
            decompressed = zlib.decompress(data)
            self.event["size"] = len(decompressed)

            # Send extracted file back to Strelka
            self.emit_file(decompressed, name=file.name)
        except zlib.error:
            self.flags.append(
                f"{self.__class__.__name__} Exception: Invalid compression or decompression data."
            )
            return
        except Exception as e:
            self.flags.append(f"{self.__class__.__name__} Exception: {str(e)[:50]}")
            return
