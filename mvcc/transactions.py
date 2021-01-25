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

    @abstractmethod
    def record_is_locked(self, record) -> bool:
        pass

    @abstractmethod
    def record_is_visible(self, record) -> bool:
        pass

    @abstractmethod
    def commit(self) -> bool:
        pass

    @abstractmethod
    def rollback(self) -> bool:
        pass

class Transactions(ABC):

    @abstractmethod
    def new(self) -> Transaction:
        pass
