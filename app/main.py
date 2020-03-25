import asyncio

async def redis_server(reader, writer):
    while True:
        data = await reader.read(100)
        if not data:
            break
        writer.write(b'+PONG\r\n')
        await writer.drain()
    writer.close()

async def main(host, port):
    server = await asyncio.start_server(redis_server, host, port)
    await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main('0.0.0.0', 6379))