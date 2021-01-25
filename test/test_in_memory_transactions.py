import unittest

from mvcc.in_memory_transactions import InMemoryTransactions


class TestInMemoryTransactions(unittest.TestCase):

    def test_basic(self):

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


if __name__ == '__main__':
    unittest.main()
