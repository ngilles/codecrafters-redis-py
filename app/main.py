def main():
    print("Implement your Redis server here!")

    # Uncomment this to pass the first stage
    #
    import socket
    s = socket.create_server(("localhost", 6379), reuse_port=True)
    sock, addr = s.accept() # wait for client
    sock.send(b'+PONG\r\n')


if __name__ == "__main__":
    main()