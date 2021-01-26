import typing

from mvcc.transactions import Transaction, Transactions


class InMemoryTransaction(Transaction):

    def __init__(self, in_memory_transactions, xid):
        self._in_memory_transactions = in_memory_transactions
        self._id = xid
        self._rollback_actions = []

    @property
    def in_memory_transactions(self):
        return self._in_memory_transactions

    @property
    def id(self):
        return self._id

    @property
    def rollback_actions(self):
        return self._rollback_actions

    def add_record(self, record):
        record['created_xid'] = self.id
        record['expired_xid'] = 0
        self.rollback_actions.append(["delete", len(self.in_memory_transactions.records)])
        self.in_memory_transactions.records.append(record)

    def remove_record(self, record_id):
        for i, record in enumerate(self.in_memory_transactions.records):
            if self.record_is_visible(record) and record['id'] == record_id:
                if self.record_is_locked(record):
                    raise Exception("Row locked by another transaction.")
                    # print("Locked by another transaction")
                else:
                    record['expired_xid'] = self.id
                    self.rollback_actions.append(["add", i])

    def record_is_locked(self, record):
        return record['expired_xid'] != 0 and \
               record['expired_xid'] in self.in_memory_transactions.active_ids

    def record_is_visible(self, record):
        # The record was created in active transaction that is not our
        # own.
        if record['created_xid'] in self.in_memory_transactions.active_ids and record['created_xid'] != self.id:
            return False

        # The record is expired or and no transaction holds it that is
        # our own.
        if record['expired_xid'] != 0 and \
                (record['expired_xid'] not in self.in_memory_transactions.active_ids or record['expired_xid'] == self.id):
            return False

        return True

    def commit(self) -> bool:
        self.in_memory_transactions.active_ids.discard(self.id)
        return True

    def rollback(self) -> bool:
        for action in reversed(self.rollback_actions):
            if action[0] == 'add':
                self.in_memory_transactions.records[action[1]]['expired_xid'] = 0
            elif action[0] == 'delete':
                self.in_memory_transactions.records[action[1]]['expired_xid'] = self.id
        self.in_memory_transactions.active_ids.discard(self.id)

        return True

    def fetch_all(self):
        # global records
        visible_records = []
        for record in self._in_memory_transactions.records:
            if self.record_is_visible(record):
                visible_records.append(record)
        return visible_records

    def read_committed(self):
        for record in self._in_memory_transactions.records:
            if self.record_is_visible(record):
                yield record

    def read_dirty(self):
        for record in self._in_memory_transactions.records:
            yield record


class InMemoryTransactions(Transactions):

    # next_xid = 1
    # active_ids = set()
    # records = []

    def __init__(self):
        self._next_xid = 1
        self._active_ids = set()
        self._records = list()

    @property
    def next_xid(self) -> int:
        return self._next_xid

    @property
    def active_ids(self) -> typing.Set:
        return self._active_ids

    @property
    def records(self) -> typing.List:
        return self._records

    def new(self) -> InMemoryTransaction:
        self._next_xid += 1
        self._active_ids.add(self.next_xid)
        return InMemoryTransaction(self, self.next_xid)
