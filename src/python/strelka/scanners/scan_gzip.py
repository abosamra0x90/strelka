import gzip
import io
import zlib

from strelka import strelka


class ScanGzip(strelka.Scanner):
    """Decompresses gzip files."""

    def scan(self, data, file, options, expire_at):
        try:
            with io.BytesIO(data) as gzip_io:
                with gzip.GzipFile(fileobj=gzip_io) as gzip_obj:
                    decompressed = gzip_obj.read()
                    self.event["size"] = len(decompressed)

                    uuid_part = file.name.split('___')[0] if '___' in file.name else "unknown"

                    # Send extracted file back to Strelka
                    self.emit_file(decompressed, name=f"{uuid_part}___files")
        except gzip.BadGzipFile:
            self.flags.append("bad_gzip_file")
        except zlib.error:
            self.flags.append("bad_gzip_file")
        except EOFError:
            self.flags.append("eof_error")
