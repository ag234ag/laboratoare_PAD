from dataclasses import dataclass

STORAGE_MODES = ("persistent", "transient")


def _in_range(text, low, high):
    return text.isascii() and text.isdigit() and low <= int(text) <= high


@dataclass(frozen=True)
class BrokerOptions:
    port: str
    storage: str
    db: str
    workers: str

    def errors(self):
        problems = []
        if not _in_range(self.port, 1, 65535):
            problems.append("port trebuie sa fie un numar intre 1 si 65535")
        if self.storage not in STORAGE_MODES:
            problems.append("storage trebuie sa fie persistent sau transient")
        if not self.db.strip():
            problems.append("db nu poate fi gol")
        if not _in_range(self.workers, 1, 64):
            problems.append("workers trebuie sa fie un numar intre 1 si 64")
        return problems

    def arguments(self):
        return ["--port", self.port, "--storage", self.storage, "--db", self.db, "--workers", self.workers]
