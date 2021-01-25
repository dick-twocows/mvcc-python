from abc import ABC, abstractmethod


class Transaction(ABC):

    @abstractmethod
    def add_record(self, record) -> None:
        pass

    @abstractmethod
    def remove_record(self, record_id) -> None:
        pass

    def update_record(self, record) -> None:
        self.remove_record(record['id'])
        self.add_record(record)


class Transactions(ABC):

    @abstractmethod
    def new(self) -> Transaction:
        pass
