#! python3
# client_putah.py
# usage: python3 client_putah.py --server_ip XXXX.XXXX.XXXX.XXXX --server_port YYYY

import socket
import argparse
import zlib
import struct
import random
import time
import os
from datetime import datetime


def timestamp():
    timestamp = time.time()
    return datetime.utcfromtimestamp(timestamp).strftime('%Y%m%d%H%M%S')


def file_logging(msg):
    write_file = open(log_file_path, 'a')
    write_file.write(msg + '\n')
    write_file.close()
    print(msg)


log_file = f"client_putah_{timestamp()}.log"
log_file_path = os.path.abspath(os.path.join('.', log_file))

# create argument parser
parser = argparse.ArgumentParser()

# add required server_ip argument
parser.add_argument('-s', '--server_ip', type=str, required=True)

# add the required server_port argument
parser.add_argument('-p', '--server_port', type=int, required=True)

# parse the arguments
args = parser.parse_args()

# ip address or hostname of the hose
host = args.server_ip

# port the host is listening for connections on
port = args.server_port

# port the client will use
client_port = 5002

# BUFFER_SIZE
BUFFER_SIZE = 1024

# message to be sent to server
message = "Ping"


# checksum calculator for ensuring our data is being transferred
def checksum_calc(data):
    checksum = zlib.crc32(data)
    return checksum


def connect(conn_host, conn_port):
    server = (conn_host, conn_port)
    # initialize the socket to use for incoming
    in_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # initialize the socket to use for outgoing
    out_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # bind the incoming socket to listen for connections from the server
    in_sock.bind(('localhost', client_port))
    print(f"Establishing connection to {server}")
    # first handshake sequence is random
    seq = random.getrandbits(4)
    # create the header
    first_handshake_header_p, first_handshake_header = header(client_port, conn_port, ''.encode(), seq, syn=True)
    # send the header to the server
    send(out_sock, first_handshake_header_p, server)
    in_header, in_data, in_address = receive(in_sock)
    if in_header[6] and in_header[7] and (in_header[5] == seq + 1):
        tcp_port = find_open_ports()
        second_handshake_header_p, second_handshake_header = header(tcp_port, conn_port, ''.encode(), seq,
                                                                    ack_seq=in_header[5] + 1,
                                                                    ack=True)
        send(out_sock, second_handshake_header_p, server)
        # initialize the socket to use for the connection
        tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # bind the incoming socket to listen for connections from the server
        tcp_sock.bind(('localhost', tcp_port))
        print(f"binding listener socket to localhost:{tcp_port}")

        in_sock.close()
        out_sock.close()
        return tcp_sock, (server[0], in_header[0])
    else:
        in_sock.close()
        out_sock.close()
        return None, None


def close(s, address, tries=0):
    print(f"Disconnecting from: {address}")
    try:
        s.settimeout(5)
        if tries < 3:  # wait for acknowledgement from server before closing
            data = ''.encode()
            close_header_p, close_header = header(s.getsockname()[1], address[1], data,
                                                  0, fin=True)
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                    send(s, close_header_p + data, address)
                r_header, r_data, r_address = receive(s)
                if r_header[6] and r_header[8]:
                    print("server responded to disconnect.")
                    s.close()
                else:
                    print("else")
                    close(s, address, tries + 1)
            except:
                close(s, address, tries + 1)
        else:  # server not responding, close anyway
            s.close()
        print("Finished closing connection.")
    except OSError:
        pass


def send(s, packet, address):
    s.sendto(packet, address)
    header = packet[:27]
    header = unpack_header(header)
    file_logging(f"{header[0]} | {header[1]} | {msg_type(header)} | {header[2]} | {timestamp()}")


def receive(rec_sock, buffer=1024):
    r, r_address = rec_sock.recvfrom(buffer)
    r_header = r[:27]
    r_header = unpack_header(r_header)
    data = r[27:]
    file_logging(f"{r_header[0]} | {r_header[1]} | {msg_type(r_header)} | {r_header[2]} | {timestamp()}")
    return r_header, data, r_address


def header(sender_port, receiver_port, data, seq, ack_seq=0, ack=False, syn=False, fin=False):
    # create the header ('struct format', 'sender port', 'receiver port', 'win',
    # 'checksum', 'seq', 'ack seq', 'ack', 'syn', 'fin')
    win = len(data)
    checksum = checksum_calc(data)
    if ack_seq == 0:
        ack_seq = seq
    header_packed = struct.pack("!IIIIII???", sender_port, receiver_port, win, checksum, seq,
                                ack_seq, ack, syn, fin)
    return header_packed, unpack_header(header_packed)


def unpack_header(header):
    return struct.unpack("!IIIIII???", header)


def find_open_ports():
    for p in range(6001, 7000):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            res = s.connect_ex((host, p))
            if res == 0:
                return p

def msg_type(header):
    if header[2] > 0:
        return "DATA"
    elif header[6] and header[7]:
        return "ACK/SYN"
    elif header[6]:
        return "ACK"
    elif header[7]:
        return "SYN"
    elif header[8]:
        return "FIN"


tcp_sock, address = connect(host, port)
print(f"handhsake complete, server address = {address}")
if tcp_sock:
    data = message.encode()
    seq = 1
    ack_seq = seq + 1
    while True:
        try:
            time.sleep(2)
            ping_pong_header_p, ping_pong_header = header(tcp_sock.getsockname()[1], address[1],
                                                          data, seq, ack_seq=ack_seq, ack=True)
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                send(s, ping_pong_header_p + data, address)
            r_header, r_data, r_address = receive(tcp_sock)
            r_data = r_data.decode()
            ack_seq += 1
        except KeyboardInterrupt:
            print("Keyboard Interrupt, exiting and closing connections.")
            close(tcp_sock, address)
            break
else:
    print("Server rejected the connection.")
