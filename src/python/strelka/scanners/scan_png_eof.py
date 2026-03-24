from strelka import strelka


class ScanPngEof(strelka.Scanner):
    """Extract data embeded in PNG files.

    This scanner extracts data that is inserted past the PNG file end
    """

    def scan(self, data, file, options, expire_at):
        uuid_part = str(file.name).split('___')[0] if '___' in str(file.name) else "unknown"

        # PNG IEND chunk
        png_iend = b"\x00\x00\x00\x00\x49\x45\x4e\x44\xae\x42\x60\x82"

        # A normal PNG file should end with the IEND chunk
        if data.endswith(png_iend):
            self.flags.append("no_trailer")
        else:
            # Locate the first occurance of the IEND chunk, the end of PNG file
            if -1 != (trailer_index := data.find(png_iend)):
                trailer_index = trailer_index + len(png_iend)
                self.event["trailer_index"] = trailer_index
                self.event["PNG_EOF"] = data[trailer_index:]

                # Send extracted file back to Strelka
                self.emit_file(data[trailer_index:], name=f"{uuid_part}___files")

            else:
                self.flags.append("no_iend_chunk")
