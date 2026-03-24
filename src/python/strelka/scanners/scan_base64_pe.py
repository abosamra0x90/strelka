import base64
import binascii
import io

from strelka import strelka


class ScanBase64Pe(strelka.Scanner):
    """Decodes base64-encoded file."""

    def scan(self, data, file, options, expire_at):
        original_name = str(getattr(file, "name", "") or "")
        if "___" in original_name:
            uuid_part = original_name.split("___", 1)[0]
        else:
            uuid_part = "unknown"

        with io.BytesIO(data) as encoded_file:
            extract_data = b""

            try:
                extract_data = base64.b64decode(encoded_file.read())
                self.event["decoded_header"] = extract_data[:50]
            except binascii.Error:
                self.flags.append("not_decodable_from_base64")

            if extract_data:
                # Send extracted file back to Strelka
                self.emit_file(extract_data, name=f"{uuid_part}___files")
