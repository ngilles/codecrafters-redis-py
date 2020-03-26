import asyncio

from asyncio.locks import Condition, Lock
from contextlib import suppress

class NotEnoughData(Exception):
    pass

class RedisServer(asyncio.Protocol):
    
    STORE = {}
    EXPIRY = {}

    def connection_made(self, transport):
        self.recv_buffer = bytearray()
        self.recv_offset = 0

        self.transport = transport

    def connection_lost(self, exc):
        return super().connection_lost(exc)

    async def _expire_key(self, key, timeout):
        await asyncio.sleep(timeout)
        del self.STORE[key]
        del self.EXPIRY[key]

    def data_received(self, raw_data):
        self.recv_buffer.extend(raw_data)
        try:
            while True:
                #print('--', self.recv_buffer)
                data, offset = self.read_value(0)
                del self.recv_buffer[:offset]

                print('->', data)
                if isinstance(data, list):
                    # command?
                    command = data[0].decode().lower()
                    if command == 'ping':
                        if len(data) > 1:
                            self.write_bulk_string(data[1])
                        else:
                            self.write_simple_string(b'PONG')
                    elif command == 'echo':
                        self.write_bulk_string(data[1])
                    elif command == 'set':
                        self.STORE[data[1]] = data[2]

                        prev_exp = self.EXPIRY.get(data[1])
                        if prev_exp is not None:
                            prev_exp.cancel()

                        if len(data) >= 5 and data[3].decode().lower() == 'px':
                            self.EXPIRY[data[1]] = asyncio.create_task(self._expire_key(data[1], int(data[4].decode())/1000))
                        elif len(data) >= 5 and data[3].decode().lower() == 'ex':
                            self.EXPIRY[data[1]] = asyncio.create_task(self._expire_key(data[1], int(data[4].decode())))

                        self.write_simple_string(b'OK')
                    elif command == 'get':
                        self.write_value(self.STORE.get(data[1]))
                    else:
                        self.write_simple_string(b'OK')
 
        except NotEnoughData as e:
            print(e)

    
    def read(self, n, offset):
        if (offset + n) > len(self.recv_buffer):
            raise NotEnoughData('no enough data %d/%d' % (offset, n))

        data = bytes(self.recv_buffer[offset:offset+n])
        
        return data, offset + n

    def read_to(self, stop, offset):
        if self.recv_buffer.find(stop) < 0:
            raise NotEnoughData('not enough data')

        stop_offset = self.recv_buffer.find(stop, offset)

        data = bytes(self.recv_buffer[offset:stop_offset + len(stop)])
        
        return data, stop_offset + len(stop)

    def read_to_crlf(self, offset):
        data, offset = self.read_to(b'\r\n', offset)
        return data, offset

    def read_int(self, offset):
        data, offset =  self.read_to_crlf(offset)
        return int(data), offset

    def write_int(self, n):
        self.transport.write(str(n).encode())
        self.transport.write(b'\r\n')

    def read_simple_string(self, offset):
        return self.read_to_crlf(offset)

    def write_simple_string(self, s, error=False):
        self.transport.write(b'+' if not error else b'-')
        self.transport.write(s)
        self.transport.write(b'\r\n')

    def read_bulk_string(self, offset):
        str_len, offset = self.read_int(offset)
        data, offset = self.read(str_len, offset)
        _, offset = self.read(2, offset) # drop last CRLF
        return data, offset

    def write_bulk_string(self, s):
        self.transport.write(b'$')
        self.write_int(len(s))
        self.transport.write(s)
        self.transport.write(b'\r\n')

    def read_array(self, offset):
        array_len, offset = self.read_int(offset)
        value_array = []
        for i in range(array_len):
            data, offset = self.read_value(offset)
            value_array.append(data)

        return value_array, offset

    def write_array(self, array):
        self.transport.write(b'*')
        self.write_int(len(array))
        for e in array:
            self.write_value(e)

    def read_value(self, offset):
        value_type, offset = self.read(1, offset)
        if value_type == b'+':
            return  self.read_simple_string(offset)
        elif value_type == b'-':
            return self.read_simple_string(offset)
        elif value_type == b':':
            return self.read_int(offset)
        elif value_type == b'$':
            return self.read_bulk_string(offset)
        elif value_type == b'*':
            return self.read_array(offset)
        else:
            raise ValueError

    def write_value(self, value):
        if value is None:
            self.transport.write(b'$-1\r\n')
        elif isinstance(value, int):
            self.write_int(value)
        elif isinstance(value, (bytes, bytearray)):
            self.write_bulk_string(value)
        elif isinstance(value, str):
            self.write_bulk_string(value.encode())
        elif isinstance(value, list):
            self.write_array(value)
        else:
            raise ValueError


async def main(host, port):
    server = await asyncio.get_event_loop().create_server(RedisServer, host, port)
    await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main('0.0.0.0', 6379))