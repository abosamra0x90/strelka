import base64

from strelka import strelka


class ScanBase64(strelka.Scanner):
    """Decodes base64-encoded file."""

    def scan(self, data, file, options, expire_at):
        original_name = str(getattr(file, "name", "") or "")
        if "___" in original_name:
            uuid_part = original_name.split("___", 1)[0]
        else:
            uuid_part = "unknown"

        decoded = base64.b64decode(data)

        self.event["size"] = len(decoded)

        # Send extracted file back to Strelka
        self.emit_file(decoded, name=f"{uuid_part}___files")
