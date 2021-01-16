next_xid = 1
active_xids = set()
records = []


def new_transaction():
    global next_xid
    next_xid += 1
    active_xids.add(next_xid)
    return Transaction(next_xid)


class Transaction:
    def __init__(self, xid):
        self.xid = xid
        self.rollback_actions = []

    def add_record(self, record):
        record['created_xid'] = self.xid
        record['expired_xid'] = 0
        self.rollback_actions.append(["delete", len(records)])
        records.append(record)

    def delete_record(self, id):
        for i, record in enumerate(records):
            if self.record_is_visible(record) and record['id'] == id:
                if self.row_is_locked(record):
                    # raise Error("Row locked by another transaction.")
                    print("Locked by another transaction")
                else:
                    record['expired_xid'] = self.xid
                    self.rollback_actions.append(["add", i])

    def update_record(self, id, name):
        self.delete_record(id)
        self.add_record({"id": id, "name": name})

    def row_is_locked(self, record):
        return record['expired_xid'] != 0 and \
               record['expired_xid'] in active_xids

    def record_is_visible(self, record):
        # The record was created in active transaction that is not our
        # own.
        if record['created_xid'] in active_xids and \
                record['created_xid'] != self.xid:
            return False

        # The record is expired or and no transaction holds it that is
        # our own.
        if record['expired_xid'] != 0 and \
                (record['expired_xid'] not in active_xids or record['expired_xid'] == self.xid):
            return False

        return True

    def fetch_all(self):
        global records
        visible_records = []
        for record in records:
            if self.record_is_visible(record):
                visible_records.append(record)
        return visible_records

    def commit(self):
        active_xids.discard(self.xid)

    def rollback(self):
        for action in reversed(self.rollback_actions):
            if action[0] == 'add':
                records[action[1]]['expired_xid'] = 0
            elif action[0] == 'delete':
                records[action[1]]['expired_xid'] = self.xid

        active_xids.discard(self.xid)


client1 = new_transaction()
client2 = new_transaction()
client1.add_record({"id": 1, "name": "Bob"})
client1.add_record({"id": 2, "name": "John"})
client1.delete_record(id=1)
client1.update_record(id=2, name="Tom")
print(client1.fetch_all())
print(client2.fetch_all())

client1.rollback()
print(client2.fetch_all())
