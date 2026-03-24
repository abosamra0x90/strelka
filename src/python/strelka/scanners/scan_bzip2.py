import bz2
import io

from strelka import strelka


class ScanBzip2(strelka.Scanner):
    """Decompresses bzip2 files."""

    def scan(self, data, file, options, expire_at):
        with io.BytesIO(data) as bzip2_io:
            with bz2.BZ2File(filename=bzip2_io) as bzip2_obj:
                try:
                    decompressed = bzip2_obj.read()
                    self.event["size"] = len(decompressed)

                    original_name = str(getattr(file, "name", "") or "")
                    if "___" in original_name:
                        uuid_part = original_name.split("___", 1)[0]
                    else:
                        uuid_part = "unknown"

                    # Send extracted file back to Strelka
                    self.emit_file(decompressed, name=f"{uuid_part}___files")

                except EOFError:
                    self.flags.append("eof_error")
                except OSError:
                    self.flags.append("os_error")
