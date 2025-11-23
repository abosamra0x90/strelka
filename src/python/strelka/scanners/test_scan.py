from strelka import strelka

class TestScan(strelka.Scanner):
    def scan(self, data, file, options, expire_at):
        self.event["helllllllllllllllllllllllllllllllloooooooooooooooooooooooooo"] = len(data)
