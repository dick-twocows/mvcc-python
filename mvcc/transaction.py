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

    def new(self) -> Transaction:
        raise Exception('Not implemented')


class InMemoryTransaction(Transaction):
    def __init__(self, in_memory_transactions, xid):
        self.in_memory_transactions = in_memory_transactions
        self.xid = xid
        self.rollback_actions = []

    def add_record(self, record):
        record['created_xid'] = self.xid
        record['expired_xid'] = 0
        self.rollback_actions.append(["delete", len(self.in_memory_transactions.records)])
        self.in_memory_transactions.records.append(record)

    def remove_record(self, id):
        for i, record in enumerate(self.in_memory_transactions.records):
            if self.record_is_visible(record) and record['id'] == id:
                if self.row_is_locked(record):
                    # raise Error("Row locked by another transaction.")
                    print("Locked by another transaction")
                else:
                    record['expired_xid'] = self.xid
                    self.rollback_actions.append(["add", i])

    def row_is_locked(self, record):
        return record['expired_xid'] != 0 and \
               record['expired_xid'] in self.in_memory_transactions.active_xids

    def record_is_visible(self, record):
        # The record was created in active transaction that is not our
        # own.
        if record['created_xid'] in self.in_memory_transactions.active_xids and record['created_xid'] != self.xid:
            return False

        # The record is expired or and no transaction holds it that is
        # our own.
        if record['expired_xid'] != 0 and \
                (record['expired_xid'] not in self.in_memory_transactions.active_xids or record['expired_xid'] == self.xid):
            return False

        return True

    def fetch_all(self):
        # global records
        visible_records = []
        for record in self.in_memory_transactions.records:
            if self.record_is_visible(record):
                visible_records.append(record)
        return visible_records

    def read_committed(self):
        for record in self.in_memory_transactions.records:
            if self.record_is_visible(record):
                yield record

    def read_dirty(self):
        for record in self.in_memory_transactions.records:
            yield record

    def commit(self):
        self.in_memory_transactions.active_xids.discard(self.xid)

    def rollback(self):
        for action in reversed(self.rollback_actions):
            if action[0] == 'add':
                self.in_memory_transactions.records[action[1]]['expired_xid'] = 0
            elif action[0] == 'delete':
                self.in_memory_transactions.records[action[1]]['expired_xid'] = self.xid

        self.in_memory_transactions.active_xids.discard(self.xid)


class InMemoryTransactions(Transactions):

    next_xid = 1
    active_xids = set()
    records = []

    def new(self) -> Transaction:
        self.next_xid += 1
        self.active_xids.add(self.next_xid)
        return InMemoryTransaction(self, self.next_xid)


in_memory = InMemoryTransactions()

client1 = in_memory.new()

client2 = in_memory.new()

client1.add_record({"id": 1, "name": "Bob"})
# print(client1.fetch_all())
client1.add_record({"id": 2, "name": "John"})
# print(client1.fetch_all())

print("client1 read_committed")
for i in client1.read_committed():
    print(i)
print("end")
print("client1 read_dirty")
for i in client1.read_dirty():
    print(i)
print("end")

client1.remove_record(id=1)
# print(client1.fetch_all())

client1.update_record({'id': 2, 'name': 'Tom'})
# print(client1.fetch_all())

print("client2 read_committed")
for i in client2.read_committed():
    print(i)
print("end")
print("client2 read_dirty")
for i in client2.read_dirty():
    print(i)
print("end")

# print(client2.fetch_all())

client1.rollback()
# print(client1.fetch_all())
# print(client2.fetch_all())
