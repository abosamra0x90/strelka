from strelka import strelka


class ScanBmpEof(strelka.Scanner):
    """
    Take the data of the BMP image, parse it, and determine if data is stored beyond
    the expected marker.
    """

    def scan(self, data, file, options, expire_at):
        original_name = str(getattr(file, "name", "") or "")
        if "___" in original_name:
            uuid_part = original_name.split("___", 1)[0]
        else:
            uuid_part = "unknown"

        expectedSize = int.from_bytes(data[2:6], "little")
        actualSize = len(data)
        if expectedSize != actualSize:
            self.event["trailer_index"] = expectedSize
            trailer_bytes_data = data[expectedSize:]
            self.event["BMP_EOF"] = data[expectedSize:]

            # Send extracted file back to Strelka
            self.emit_file(trailer_bytes_data, name=f"{uuid_part}___files")
        else:
            self.flags.append("no_trailer")
